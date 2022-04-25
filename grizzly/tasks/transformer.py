'''This task transforms a variable value to a document of correct type, so an expression can be used to extract a
specific value from the document to be used in another variable.

This is especially useful when used in combination with other variables variables containing a lot of information,
where many parts of a message can be useful to re-use.

Instances of this task is created with the step expression:

* [`step_task_transform`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_transform)
'''
from typing import TYPE_CHECKING, List, Callable, Any, Type, Optional

from grizzly_extras.transformer import Transformer, transformer, TransformerContentType, TransformerError

from ..context import GrizzlyContext
from ..exceptions import TransformerLocustError
from . import GrizzlyTask, template

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


@template('content')
class TransformerTask(GrizzlyTask):
    expression: str
    variable: str
    content: str
    content_type: TransformerContentType

    _transformer: Type[Transformer]
    _parser: Callable[[Any, Any], List[str]]

    def __init__(
        self,
        expression: str,
        variable: str,
        content: str,
        content_type: TransformerContentType,
        scenario: Optional['GrizzlyContextScenario'] = None,
    ) -> None:
        super().__init__(scenario)

        self.expression = expression
        self.variable = variable
        self.content = content
        self.content_type = content_type

        grizzly = GrizzlyContext()
        if self.variable not in grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: {self.variable} has not been initialized')

        _transformer = transformer.available.get(self.content_type, None)

        if _transformer is None:
            raise ValueError(f'{self.__class__.__name__}: could not find a transformer for {self.content_type.name}')

        self._transformer = _transformer

        if not self._transformer.validate(self.expression):
            raise ValueError(f'{self.__class__.__name__}: {self.expression} is not a valid expression for {self.content_type.name}')

        setattr(self, '_parser', self._transformer.parser(self.expression))

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            content_raw = parent.render(self.content)

            try:
                content = self._transformer.transform(content_raw)
            except TransformerError as e:
                parent.logger.error(f'failed to transform as {self.content_type.name}: {content_raw}')
                raise TransformerLocustError(f'{self.__class__.__name__}: failed to transform {self.content_type.name}') from e

            values = self._parser(content)

            number_of_values = len(values)

            if number_of_values != 1:
                parent.logger.error(f'"{self.expression}" returned {number_of_values} matches for: {content_raw}')
                raise RuntimeError(f'{self.__class__.__name__}: "{self.expression}" returned {number_of_values} matches')

            value = values[0]

            parent.user._context['variables'][self.variable] = value

        return task
