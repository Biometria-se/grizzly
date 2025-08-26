from __future__ import annotations  # noqa: D100

from abc import ABC, abstractmethod
from contextlib import suppress
from enum import Enum, EnumMeta
from json import JSONDecodeError
from json import loads as jsonloads
from typing import TYPE_CHECKING, Any

from dateutil.parser import ParserError
from dateutil.parser import parse as date_parse

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable


class permutation:
    """Decorator used to annotate `parse` methods that are not using [`PermutationEnum`][grizzly_common.text.PermutationEnum] as a base.

    This could be for example parse methods that uses regular expressions via `parse.with_pattern`.

    ```python
    import parse

    from behave import register_type
    from grizzly_common.text import permutation

    @parse.with_pattern(r'(hello|world)')
    @permutation(vector=(True, True))
    def parse_hello_world(text: str) -> str:
        return text.strip()

    register_type(
        HelloWorldType=parse_hello_world,
    )
    ```

    See [`__vector__`][grizzly_common.text.PermutationEnum.__vector__] for an explanation of possible values and their meaning.
    """

    vector: tuple[bool, bool] | None

    def __init__(self, *, vector: tuple[bool, bool] | None) -> None:
        self.vector = vector

    def __call__(self, func: Callable[[str], Any]) -> Callable[[str], Any]:
        setattr(func, '__vector__', self.vector)  # noqa: B010

        return func


class PermutationMeta(ABC, EnumMeta):
    pass


class PermutationEnum(Enum, metaclass=PermutationMeta):
    """Interface class for getting `__vector__` value from the class that inherits it.

    All objects used to represent possible values in step expressions and that has a registered custom `parse` type *should*
    inherit this class and set appropiate `__vector__` values and make an implementation of `from_string`. This is so the
    language server can make educated suggestions on possible step expressions.
    """

    __vector__: tuple[bool, bool] | None
    """
    This class variable represents `(x, y)` dimensions on how the values can expand in a step expression.

    Consider the following `Enum`, being mapped to a custom `parse` type named `FruitType`:

    ```python
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

    ##### None

    Variable occurs `1..N` times in the expression

    When permutated, it will only produce one step expression and all instances of the variable in the expression will have been replaced with nothing.

    ```gherkin
    Then I want to eat a "{fruit:FruitType}"  # -->

    Then I want to eat a ""

    Then I want to eat a "{fruit1:FruitType}" and a "{fruit2:FruitType}"  # -->

    Then I want to eat a "" and a ""
    ```

    ##### (False, True)

    Variable occurs `1` time in the expression.

    When permutated, it will produce the number of step expressions as values in the enum.

    ```gherkin
    Then I want to eat a {fruit:FruitType}  # -->

    Then I want to eat a banana
    Then I want to eat a apple
    Then I want to eat a orange
    ```

    ##### (True, False)

    Variable occurs `2..N` times in the expression.

    When permutated, combination of all enum values will be produced, if the variable type occurs the same number of times as values in the enum.

    ```gherkin
    Then I want to eat a {fruit1:FruitType}, a {fruit2:FruitType} and a {fruit3:FruitType}  # -->

    Then I want to eat a banana, a apple and a orange
    ```

    ##### (True, True)

    Variable occurs `2..N` times in the expression, and should produce more than one combination of the step expression.

    ```gherkin
    Then I want to eat a {fruit1:FruitType}, a {fruit2:FruitType} and a {fruit3:FruitType}  # -->

    Then I want to eat a banana, a apple and a orange
    Then I want to eat a apple, a banana and a orange
    Then I want to eat a orange, a banana and a apple
    ```
    """

    @classmethod
    def get_vector(cls) -> tuple[bool, bool] | None:
        return getattr(cls, '__vector__', None)

    @classmethod
    @abstractmethod
    def from_string(cls, value: str) -> Enum:
        message = f'{cls.__name__} has not implemented `from_string`'
        raise NotImplementedError(message)  # pragma: no cover

    @abstractmethod
    def get_value(self) -> str:
        message = f'{self.__class__.__name__} has not implemented `get_value`'
        raise NotImplementedError(message)  # pragma: no cover


def has_sequence(sequence: str, value: str) -> bool:
    """Test string for a sequence of characters, and that it only occurs once in the string."""
    return sequence in value and value.index(sequence) == value.rindex(sequence)


def has_separator(separator: str, value: str) -> bool:
    """Test string for separator, which is not connected to any other operators."""
    operators = ['=', '|']

    try:
        left_index = value.index(separator)
    except ValueError:
        return False

    try:
        return value[left_index + 1] not in operators
    except IndexError:
        return separator in value


def caster(value: Any) -> Any:
    with suppress(JSONDecodeError):
        value = jsonloads(value)

    if isinstance(value, str):
        with suppress(ParserError):
            value = date_parse(value)

    return value


def bool_caster(value: str) -> bool:
    assert value in ['True', 'False'], f'{value} is not a valid boolean'

    return value == 'True'
