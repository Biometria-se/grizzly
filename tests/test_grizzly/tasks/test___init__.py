from os import environ

import pytest

from grizzly.tasks import GrizzlyTask


class TestGrizzlyTask:
    def test___init__(self) -> None:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass

        task = GrizzlyTask()

        assert task._context_root == '.'

        try:
            environ['GRIZZLY_CONTEXT_ROOT'] = 'foo bar!'
            task = GrizzlyTask()

            assert task._context_root == 'foo bar!'
        finally:
            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except KeyError:
                pass

    def test___call__(self) -> None:
        task = GrizzlyTask()

        with pytest.raises(NotImplementedError) as nie:
            task()
        assert 'GrizzlyTask has not been implemented' == str(nie.value)

    def test_get_templates(self) -> None:
        # @TODO implement
        pass
