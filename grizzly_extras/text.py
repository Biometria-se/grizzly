from abc import ABC, abstractmethod
from typing import Tuple, Optional, Callable, Any
from enum import Enum, EnumMeta


class permutation:
    """
    Decorator used to annotate `parse` methods that are not using {@pylink grizzly_extras.text.PermutationEnum} as a base.
    This could be for example parse methods that uses regular expressions via `parse.with_pattern`.

    ``` python
    import parse

    from behave import register_type
    from grizzly_extras.text import permutation

    @parse.with_pattern(r'(hello|world)')
    @permutation(vector=(True, True,))
    def parse_hello_world(text: str) -> str:
        return text.strip()

    register_type(
        HelloWorldType=parse_hello_world,
    )
    ```

    See {@pylink grizzly_extras.text.PermutationEnum.__vector__} for an explanation of possible values and their meaning.
    """
    vector: Optional[Tuple[bool, bool]]

    def __init__(self, *, vector: Optional[Tuple[bool, bool]]) -> None:
        self.vector = vector

    def __call__(self, func: Callable[[str], Any]) -> Callable[[str], Any]:
        setattr(func, '__vector__', self.vector)

        return func


class PermutationMeta(ABC, EnumMeta):
    pass


class PermutationEnum(Enum, metaclass=PermutationMeta):
    """
    Interface class for getting `__vector__` value from the class that inherits it.

    All objects used to represent possible values in step expressions and that has a registered custom `parse` type *should*
    inherit this class and set appropiate `__vector__` values and make an implementation of `from_string`. This is so
    [`grizzly-ls`](https://github.com/Biometria-se/grizzly-lsp) can make educated suggestions on possible step expressions.
    """

    __vector__: Optional[Tuple[bool, bool]]
    """
    This class variable represents `(x, y)` dimensions on how the values can expand in a step expression.

    Consider the following `Enum`, being mapped to a custom `parse` type named `FruitType`:

    ``` python
    from behave import register_type


    class Fruit(PermutationEnum):
        __vector__ = None  # see examples below

        BANANA = 0
        APPLE = 1
        ORANGE = 2

        @classmethod
        def from_string(cls, value: str) -> Fruit:
            return cls[value.upper()]

    register_type(
        FruitType=Fruit.from_string,
    )
    ```

    ##### `None`

    Variable occurs `1..N` times in the expression

    When permutated, it will only produce one step expression and all instances of the variable in the expression will have been replaced with nothing.

    ``` gherkin
    Then I want to eat a "{fruit:FruitType}"  # -->

    Then I want to eat a ""

    Then I want to eat a "{fruit1:FruitType}" and a "{fruit2:FruitType}"  # -->

    Then I want to eat a "" and a ""
    ```

    ##### `(False, True,)`

    Variable occurs `1` time in the expression.

    When permutated, it will produce the number of step expressions as values in the enum.

    ``` gherkin
    Then I want to eat a {fruit:FruitType}  # -->

    Then I want to eat a banana
    Then I want to eat a apple
    Then I want to eat a orange
    ```

    ##### `(True, False,)`

    Variable occurs `2..N` times in the expression.

    When permutated, combination of all enum values will be produced, if the variable type occurs the same number of times as values in the enum.

    ``` gherkin
    Then I want to eat a {fruit1:FruitType}, a {fruit2:FruitType} and a {fruit3:FruitType}  # -->

    Then I want to eat a banana, a apple and a orange
    ```

    ##### `(True, True,)`

    Variable occurs `2..N` times in the expression, and should produce more than one combination of the step expression.

    ``` gherkin
    Then I want to eat a {fruit1:FruitType}, a {fruit2:FruitType} and a {fruit3:FruitType}  # -->

    Then I want to eat a banana, a apple and a orange
    Then I want to eat a apple, a banana and a orange
    Then I want to eat a orange, a banana and a apple
    ```
    """

    @classmethod
    def get_vector(cls) -> Optional[Tuple[bool, bool]]:
        return getattr(cls, '__vector__', None)

    @classmethod
    @abstractmethod
    def from_string(cls, value: str) -> Enum:
        raise NotImplementedError(f'{cls.__name__} has not implemented `from_string`')
