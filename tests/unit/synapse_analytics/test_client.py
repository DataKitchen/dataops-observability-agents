import pytest

from agents.synapse_analytics.client import _ClientWrapper


@pytest.mark.unit()
def test_get_unitialized_client():
    wrapper = _ClientWrapper()
    with pytest.raises(ValueError, match="not"):
        wrapper()
