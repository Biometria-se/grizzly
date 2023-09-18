from typing import Any, Dict, Tuple, Optional, List, cast
from abc import ABC, abstractmethod
from json import dumps as jsondumps

from grizzly_extras.transformer import transformer, TransformerError, PlainTransformer, TransformerContentType

from grizzly.types import HandlerContextType, GrizzlyResponseContextManager, GrizzlyResponse
from grizzly.types.locust import Environment
from grizzly.tasks import RequestTask
from grizzly.exceptions import ResponseHandlerError, TransformerLocustError
from grizzly.context import GrizzlyContext

from .grizzly_user import GrizzlyUser
from .response_event import ResponseEvent


class ResponseHandlerAction(ABC):
    grizzly = GrizzlyContext()

    def __init__(self, /, expression: str, match_with: str, expected_matches: str = '1', as_json: bool = False) -> None:
        self.expression = expression
        self.match_with = match_with
        self.expected_matches = expected_matches
        self.as_json = as_json

    @abstractmethod
    def __call__(
        self,
        input_context: Tuple[TransformerContentType, Any],
        user: GrizzlyUser,
        response: Optional[GrizzlyResponseContextManager] = None,
    ) -> None:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented __call__')  # pragma: no cover

    def get_match(
        self,
        input_context: Tuple[TransformerContentType, Any],
        user: GrizzlyUser,
        condition: bool = False
    ) -> Tuple[Optional[str], str, str]:
        '''Contains common logic for both save and validation handlers.

        Args:
            input_context (Tuple[TransformerContentType, Any]): content type and transformed payload
            expression (str): expression to extract value from `input_context`
            match_with (str): regular expression that the extracted value must match
            user (ContextVariablesUser): user that executed task (request)
            condition (bool): used by validation handler for negative matching
        '''
        input_content_type, input_payload = input_context
        j2env = self.grizzly.state.jinja2
        rendered_expression = j2env.from_string(self.expression).render(user.context_variables)
        rendered_match_with = j2env.from_string(self.match_with).render(user.context_variables)
        rendered_expected_matches = int(j2env.from_string(self.expected_matches).render(user.context_variables))

        try:
            transform = transformer.available.get(input_content_type, None)
            if transform is None:
                raise TypeError(f'could not find a transformer for {input_content_type.name}')

            if not transform.validate(rendered_expression):
                raise TypeError(f'"{rendered_expression}" is not a valid expression for {input_content_type.name}')

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
        except TransformerError as e:
            raise TransformerLocustError(e.message) from e

        number_of_matches = len(matches)

        if rendered_expected_matches == -1 and number_of_matches < 1:
            user.logger.error(f'"{rendered_expression}": "{rendered_match_with}" matched no values')
            match = None
        elif rendered_expected_matches > -1 and number_of_matches != rendered_expected_matches:
            if number_of_matches < rendered_expected_matches and not condition:
                user.logger.error(f'"{rendered_expression}": "{rendered_match_with}" matched too few values: "{values}"')
            elif number_of_matches > rendered_expected_matches:
                user.logger.error(f'"{rendered_expression}": "{rendered_match_with}" matched too many values: "{values}"')

            match = None
        else:
            if number_of_matches == 1:
                match = matches[0]

                if self.as_json:
                    match = jsondumps([match])
            else:
                if self.as_json:
                    match = jsondumps(matches)
                else:
                    match = '\n'.join(matches)

        return match, rendered_expression, rendered_match_with


class ValidationHandlerAction(ResponseHandlerAction):
    def __init__(self, condition: bool, /, expression: str, match_with: str, expected_matches: str = '1', as_json: bool = False) -> None:
        super().__init__(
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )

        self.condition = condition

    def __call__(
        self,
        input_context: Tuple[TransformerContentType, Any],
        user: GrizzlyUser,
        response: Optional[GrizzlyResponseContextManager] = None,
    ) -> None:
        match, expression, match_with = self.get_match(input_context, user, self.condition)

        result = match is not None if self.condition is True else match is None

        if result:
            failure = (user._scenario.failure_exception or ResponseHandlerError)(f'"{expression}": "{match_with}" was {match}')
            if response is not None:
                response.failure(failure)
            else:
                raise failure


class SaveHandlerAction(ResponseHandlerAction):
    def __init__(self, variable: str, /, expression: str, match_with: str, expected_matches: str = '1', as_json: bool = False) -> None:
        super().__init__(
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )

        self.variable = variable

    def __call__(
        self,
        input_context: Tuple[TransformerContentType, Any],
        user: GrizzlyUser,
        response: Optional[GrizzlyResponseContextManager] = None,
    ) -> None:
        match, expression, _ = self.get_match(input_context, user)

        user.set_context_variable(self.variable, match)

        if match is None:
            failure = (user._scenario.failure_exception or ResponseHandlerError)(f'"{expression}" did not match value')
            if response is not None:
                response.failure(failure)
            else:
                raise failure


class ResponseHandler(ResponseEvent):
    abstract: bool = True

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self.response_event.add_listener(self.response_handler)

    def response_handler(
        self,
        name: str,
        context: HandlerContextType,
        request: RequestTask,
        user: GrizzlyUser,
        **kwargs: Dict[str, Any],
    ) -> None:
        if getattr(request, 'response', None) is None:
            return

        handlers = request.response.handlers

        # check if there's anything todo
        if len(handlers.payload) < 1 and len(handlers.metadata) < 1:
            return

        response_metadata: Optional[Dict[str, Any]]
        response_payload: Optional[str]

        if isinstance(context, getattr(GrizzlyResponseContextManager, '__args__')):
            response_payload = context.text
            response_metadata = dict(context.headers)
            response_context = context
        else:
            response_metadata, response_payload = cast(GrizzlyResponse, context)
            response_context = None

        if len(handlers.payload) > 0:
            try:
                # do not guess which transformer to use
                impl = transformer.available.get(request.response.content_type, None)
                if impl is not None:
                    response_payload = impl.transform(response_payload or '')
                else:
                    raise TransformerError(f'failed to transform: {response_payload} with content type {request.response.content_type.name}')
            except TransformerError as e:
                if response_context is not None:
                    response_context.failure(e.message)
                    return

                raise ResponseHandlerError(e.message) from e

            for handler in handlers.payload:
                handler((request.response.content_type, response_payload), user, response_context)

        if len(handlers.metadata) > 0:
            for handler in handlers.metadata:
                handler((TransformerContentType.JSON, response_metadata or {}), user, response_context)
