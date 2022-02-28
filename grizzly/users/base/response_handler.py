from typing import Any, Dict, Tuple, Optional

from locust.clients import ResponseContextManager

from ...task import RequestTask
from ...types import HandlerContextType
from ...exceptions import ResponseHandlerError
from .grizzly_user import GrizzlyUser
from .response_event import ResponseEvent

from grizzly_extras.transformer import transformer, TransformerError, TransformerContentType

class ResponseHandler(ResponseEvent):
    abstract = True

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

        self.response_event.add_listener(self.response_handler)

    def response_handler(
        self,
        name: str,
        context: HandlerContextType,
        request: RequestTask,
        user: GrizzlyUser,
        **_kwargs: Dict[str, Any],
    ) -> None:
        if getattr(request, 'response', None) is None:
            return

        handlers = request.response.handlers

        # check if there's anything todo
        if len(handlers.payload) < 1 and len(handlers.metadata) < 1:
            return

        response_metadata: Optional[Dict[str, Any]]
        response_payload: str

        if isinstance(context, ResponseContextManager):
            response_payload = context.text
            response_metadata = dict(context.headers)
            response_context = context
        else:
            response_metadata, response_payload = context
            response_context = None

        if len(handlers.payload) > 0 and response_payload is not None and len(response_payload) > 0:
            try:
                # do not guess which transformer to use
                impl = transformer.available.get(request.response.content_type, None)
                if impl is not None:
                    response_payload = impl.transform(response_payload)
                else:
                    raise TransformerError(f'failed to transform: {response_payload} with content type {request.response.content_type.name}')
            except TransformerError as e:
                if response_context is not None:
                    response_context.failure(e.message)
                    return

                raise ResponseHandlerError(e.message) from e

            for handler in handlers.payload:
                handler((request.response.content_type, response_payload), user, response_context)

        if len(handlers.metadata) > 0 and response_metadata is not None:
            for handler in handlers.metadata:
                handler((TransformerContentType.JSON, response_metadata), user, response_context)