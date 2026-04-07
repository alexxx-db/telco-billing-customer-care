from mlflow.deployments import get_deploy_client


def _query_endpoint(endpoint_name: str, messages: list[dict[str, str]],
                    max_tokens: int, persona: str = "customer_care") -> list[dict[str, str]]:
    """Calls a model serving endpoint with persona context via custom_inputs."""
    try:
        res = get_deploy_client('databricks').predict(
            endpoint=endpoint_name,
            inputs={
                'messages': messages,
                "max_tokens": max_tokens,
                "custom_inputs": {"persona": persona},
            },
        )
    except ConnectionError as e:
        raise RuntimeError(
            f"Could not connect to serving endpoint '{endpoint_name}'. "
            f"Check that the endpoint is running and accessible."
        ) from e
    except TimeoutError as e:
        raise RuntimeError(
            f"Request to serving endpoint '{endpoint_name}' timed out."
        ) from e

    if "messages" in res:
        return res["messages"]
    elif "choices" in res:
        return [res["choices"][0]["message"]]
    raise RuntimeError(
        "Unexpected response format from serving endpoint. "
        "This app requires a Databricks agent serving endpoint or a "
        "foundation model endpoint with the chat task type."
    )


def query_endpoint(endpoint_name, messages, max_tokens, persona="customer_care"):
    return _query_endpoint(endpoint_name, messages, max_tokens, persona=persona)[-1]
