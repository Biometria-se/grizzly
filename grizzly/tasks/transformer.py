"""@anchor pydoc:grizzly.tasks.transformer Transformer
This task transforms a variable value to a document of correct type, so an expression can be used to extract
values from the document to be used in another variable.

This is especially useful when used in combination with other variables variables containing a lot of information,
where many parts of a message can be useful to re-use.

Instances of this task is created with the step expression:

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.transformer.step_task_transform}

## Statistics

Executions of this task will **not** be visible in `locust` request statistics, *unless* something goes wrong. It will
then have the request type `TRNSF`.

## Arguments

* `contents` _str_ - text to parse, supports {@link framework.usage.variables.templating} or a static string

* `content_type` _TransformerContentType_ - MIME type of `contents`, which transformer to use

* `expression` _str_ - JSON- or XPath expression to extract specific values in `contents`

* `variable` _str_ - name of variable to save value to, must have been intialized
"""
from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable

from grizzly.exceptions import failure_handler
from grizzly_extras.transformer import Transformer, TransformerContentType, TransformerError, transformer

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('content', 'expression')
class TransformerTask(GrizzlyTask):
    expression: str
    variable: str
    content: str
    content_type: TransformerContentType

    _transformer: type[Transformer]
    _parser: Callable[[Any], list[str]]

    def __init__(
        self,
        expression: str,
        variable: str,
        content: str,
        content_type: TransformerContentType,
    ) -> None:
        super().__init__(timeout=None)

        self.expression = expression
        self.variable = variable
        self.content = content
        self.content_type = content_type

        assert self.variable in self.grizzly.scenario.variables, f'{self.__class__.__name__}: {self.variable} has not been initialized'

        _transformer = transformer.available.get(self.content_type, None)

        assert _transformer is not None, f'{self.__class__.__name__}: could not find a transformer for {self.content_type.name}'

        self._transformer = _transformer

        assert self._transformer.validate(self.expression), f'{self.__class__.__name__}: {self.expression} is not a valid expression for {self.content_type.name}'

        self._parser = self._transformer.parser(self.expression)

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            start = perf_counter()
            response_length = 0

            try:
                content_raw = parent.user.render(self.content)
                response_length = len(content_raw)

                try:
                    content = self._transformer.transform(content_raw)
                except TransformerError:
                    message = f'failed to transform {self.content_type.name}'
                    parent.logger.exception('%s: %s', message, content_raw)
                    raise

                values = self._parser(content)

                number_of_values = len(values)

                if number_of_values < 1:
                    message = f'"{self.expression}" returned {number_of_values} matches'
                    parent.logger.error('%s: %s', message, content_raw)
                    raise RuntimeError(message)

                value = '\n'.join(values)

                parent.user.set_variable(self.variable, value)
            except Exception as exception:
                response_time = int((perf_counter() - start) * 1000)
                parent.user.environment.events.request.fire(
                    request_type='TRNSF',
                    name=f'{parent.user._scenario.identifier} Transformer=>{self.variable}',
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

                failure_handler(exception, parent.user._scenario)

        return task
