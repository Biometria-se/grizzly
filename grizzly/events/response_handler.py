"""Abstract load user that handles responses."""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import suppress
from json import dumps as jsondumps
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from grizzly.context import GrizzlyContext
from grizzly.events import GrizzlyEventHandler
from grizzly.exceptions import ResponseHandlerError
from grizzly_extras.transformer import PlainTransformer, TransformerContentType, TransformerError, transformer

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse, HandlerContextType
    from grizzly.users import GrizzlyUser

class ResponseHandlerAction(ABC):
    grizzly = GrizzlyContext()

    def __init__(self, /, expression: str, match_with: str, expected_matches: str = '1', *, as_json: bool = False) -> None:
        self.expression = expression
        self.match_with = match_with
        self.expected_matches = expected_matches
        self.as_json = as_json

    @abstractmethod
    def __call__(
        self,
        input_context: Tuple[TransformerContentType, HandlerContextType],
        user: GrizzlyUser,
    ) -> None:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented __call__'
        raise NotImplementedError(message)

    def get_match(
        self,
        input_context: Tuple[TransformerContentType, HandlerContextType],
        user: GrizzlyUser,
        *,
        condition: bool = False,
    ) -> Tuple[Optional[str], str, str]:
        """Contains common logic for both save and validation handlers.

        Args:
            input_context (Tuple[TransformerContentType, Any]): content type and transformed payload
            expression (str): expression to extract value from `input_context`
            match_with (str): regular expression that the extracted value must match
            user (ContextVariablesUser): user that executed task (request)
            condition (bool): used by validation handler for negative matching
        """
        input_content_type, input_payload = input_context
        j2env = self.grizzly.state.jinja2
        rendered_expression = j2env.from_string(self.expression).render(user.context_variables)
        rendered_match_with = j2env.from_string(self.match_with).render(user.context_variables)
        rendered_expected_matches = int(j2env.from_string(self.expected_matches).render(user.context_variables))

        transform = transformer.available.get(input_content_type, None)
        if transform is None:
            message = f'could not find a transformer for {input_content_type.name}'
            raise TypeError(message)

        if not transform.validate(rendered_expression):
            message = f'"{rendered_expression}" is not a valid expression for {input_content_type.name}'
            raise TypeError(message)

        input_get_values = transform.parser(rendered_expression)
        match_get_values = PlainTransformer.parser(rendered_match_with)

        values = input_get_values(input_payload)

        # get a list of all matches in values
        matches: List[str] = []
        for value in values:
            matched_values = match_get_values(value)

            if len(matched_values) < 1:
                continue

            matched_value = matched_values[0]

            if matched_value is None or len(matched_value) < 1:
                continue

            matches.append(matched_value)

        number_of_matches = len(matches)

        if rendered_expected_matches == -1 and number_of_matches < 1:
            user.logger.error('"%s": "%s" matched no values', rendered_expression, rendered_match_with)
            match = None
        elif rendered_expected_matches > -1 and number_of_matches != rendered_expected_matches:
            if number_of_matches < rendered_expected_matches and not condition:
                user.logger.error('"%s": "%s" matched too few values: "%r"', rendered_expression, rendered_match_with, values)
            elif number_of_matches > rendered_expected_matches:
                user.logger.error('"%s": "%s" matched too many values: "%r"', rendered_expression, rendered_match_with, values)

            match = None
        elif number_of_matches == 1:
            match = matches[0]

            if self.as_json:
                match = jsondumps([match])
        elif self.as_json:
            match = jsondumps(matches)
        else:
            match = '\n'.join(matches)

        return match, rendered_expression, rendered_match_with


class ValidationHandlerAction(ResponseHandlerAction):
    def __init__(self, /, expression: str, match_with: str, expected_matches: str = '1', *, condition: bool, as_json: bool = False) -> None:
        super().__init__(
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )

        self.condition = condition

    def __call__(
        self,
        input_context: Tuple[TransformerContentType, HandlerContextType],
        user: GrizzlyUser,
    ) -> None:
        match, expression, match_with = self.get_match(input_context, user, condition=self.condition)

        result = match is not None if self.condition is True else match is None

        if result:
            message = f'"{expression}": "{match_with}" was {match}'
            raise ResponseHandlerError(message)


class SaveHandlerAction(ResponseHandlerAction):
    def __init__(self, variable: str, /, expression: str, match_with: str, expected_matches: str = '1', *, as_json: bool = False) -> None:
        super().__init__(
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )

        self.variable = variable

    def __call__(
        self,
        input_context: Tuple[TransformerContentType, HandlerContextType],
        user: GrizzlyUser,
    ) -> None:
        match, expression, _ = self.get_match(input_context, user)

        user.set_context_variable(self.variable, match)

        if match is None:
            message = f'"{expression}" did not match value'
            raise ResponseHandlerError(message)


class ResponseHandler(GrizzlyEventHandler):
    def __call__(
        self,
        name: str,
        context: GrizzlyResponse,
        request: RequestTask,
        exception: Optional[Exception] = None,  # noqa: ARG002
        **_kwargs: Any,
    ) -> None:
        if getattr(request, 'response', None) is None:
            return

        handlers = request.response.handlers

        # check if there's anything todo
        if len(handlers.payload) < 1 and len(handlers.metadata) < 1:
            return

        response_metadata: Optional[Dict[str, Any]]
        response_payload: Optional[str]

        response_metadata, response_payload = context

        try:
            if len(handlers.payload) > 0:
                try:
                    # do not guess which transformer to use
                    impl = transformer.available.get(request.response.content_type, None)
                    if impl is not None:
                        response_payload = impl.transform(response_payload or '')
                    else:
                        message = f'failed to transform: {response_payload} with content type {request.response.content_type.name}'
                        raise TransformerError(message)
                except TransformerError as e:
                    raise ResponseHandlerError(e.message) from e

                for handler in handlers.payload:
                    handler((request.response.content_type, response_payload), self.user)

            for handler in handlers.metadata:
                handler((TransformerContentType.JSON, response_metadata or {}), self.user)
        except Exception as e:
            self.user.logger.exception('response handler failure')
            # do not execute response handler again
            for _handler in self.event_hook._handlers:
                if isinstance(_handler, self.__class__):
                    continue

                with suppress(Exception):
                    _handler(
                        name=name,
                        request=request,
                        context=context,
                        exception=e,
                    )
            raise
