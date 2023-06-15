'''
@anchor pydoc:grizzly.tasks.transformer Transformer
This task transforms a variable value to a document of correct type, so an expression can be used to extract
values from the document to be used in another variable.

This is especially useful when used in combination with other variables variables containing a lot of information,
where many parts of a message can be useful to re-use.

Instances of this task is created with the step expression:

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_transform}

## Statistics

Executions of this task will **not** be visible in `locust` request statistics, *unless* something goes wrong. It will
then have the request type `TRNSF`.

## Arguments

* `contents` _str_ - text to parse, supports {@link framework.usage.variables.templating} or a static string

* `content_type` _TransformerContentType_ - MIME type of `contents`, which transformer to use

* `expression` _str_ - JSON- or XPath expression to extract specific values in `contents`

* `variable` _str_ - name of variable to save value to, must have been intialized
'''
from time import perf_counter
from typing import TYPE_CHECKING, List, Callable, Any, Type

from grizzly_extras.transformer import Transformer, transformer, TransformerContentType, TransformerError

from grizzly.exceptions import TransformerLocustError

from . import GrizzlyTask, template, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.scenarios import GrizzlyScenario


@template('content')
class TransformerTask(GrizzlyTask):
    expression: str
    variable: str
    content: str
    content_type: TransformerContentType

    _transformer: Type[Transformer]
    _parser: Callable[[Any], List[str]]

    def __init__(
        self,
        grizzly: 'GrizzlyContext',
        expression: str,
        variable: str,
        content: str,
        content_type: TransformerContentType,
    ) -> None:
        super().__init__()

        self.expression = expression
        self.variable = variable
        self.content = content
        self.content_type = content_type

        if self.variable not in grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: {self.variable} has not been initialized')

        _transformer = transformer.available.get(self.content_type, None)

        if _transformer is None:
            raise ValueError(f'{self.__class__.__name__}: could not find a transformer for {self.content_type.name}')

        self._transformer = _transformer

        if not self._transformer.validate(self.expression):
            raise ValueError(f'{self.__class__.__name__}: {self.expression} is not a valid expression for {self.content_type.name}')

        setattr(self, '_parser', self._transformer.parser(self.expression))

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: 'GrizzlyScenario') -> Any:
            start = perf_counter()
            response_length = 0

            try:
                content_raw = parent.render(self.content)
                response_length = len(content_raw)

                try:
                    content = self._transformer.transform(content_raw)
                except TransformerError as e:
                    parent.logger.error(f'failed to transform as {self.content_type.name}: {content_raw}')
                    raise TransformerLocustError(f'failed to transform {self.content_type.name}') from e

                values = self._parser(content)

                number_of_values = len(values)

                if number_of_values < 1:
                    parent.logger.error(f'"{self.expression}" returned {number_of_values} matches for: {content_raw}')
                    raise RuntimeError(f'"{self.expression}" returned {number_of_values} matches')

                value = '\n'.join(values)

                parent.user._context['variables'][self.variable] = value
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

                if exception is not None and parent.user._scenario.failure_exception is not None:
                    raise parent.user._scenario.failure_exception()

        return task
