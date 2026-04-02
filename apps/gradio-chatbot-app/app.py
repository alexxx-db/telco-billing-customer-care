"""
Gradio chat UI for the billing agent — runs on Databricks Apps.

Environment: SERVING_ENDPOINT (injected via app.yaml from serving-endpoint resource).
Databricks Apps sets GRADIO_SERVER_PORT and GRADIO_SERVER_NAME for Gradio.
"""
import os

import gradio as gr

from model_serving_utils import query_endpoint

SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT")
if not SERVING_ENDPOINT:
    raise RuntimeError("SERVING_ENDPOINT must be set (configure serving-endpoint in app resources).")

PERSONA_CHOICES = [
    ("Customer Support", "customer_care"),
    ("Finance & Analytics", "finance_ops"),
    ("Executive View", "executive"),
    ("Platform Engineering", "technical"),
]


def _messages_from_history(history: list, user_msg: str) -> list[dict]:
    messages: list[dict] = []
    for pair in history or []:
        if isinstance(pair, (list, tuple)) and len(pair) >= 2:
            messages.append({"role": "user", "content": str(pair[0])})
            messages.append({"role": "assistant", "content": str(pair[1])})
    messages.append({"role": "user", "content": user_msg})
    return messages


def _reply(user_msg: str, history: list, persona_key: str) -> tuple:
    if not (user_msg or "").strip():
        return history, ""
    messages = _messages_from_history(history, user_msg.strip())
    out = query_endpoint(SERVING_ENDPOINT, messages, 1024, persona=persona_key)
    text = out.get("content", str(out)) if isinstance(out, dict) else str(out)
    new_hist = list(history or []) + [[user_msg.strip(), text]]
    return new_hist, ""


def build_ui() -> gr.Blocks:
    with gr.Blocks(title="Billing AI Assistant") as demo:
        gr.Markdown("# Billing AI Assistant")
        gr.Markdown(
            "Gradio UI for the same agent as the Dash app. "
            "Persona controls tool policy via `custom_inputs` on the serving endpoint."
        )
        persona = gr.Dropdown(
            choices=[(label, key) for label, key in PERSONA_CHOICES],
            value="customer_care",
            label="Persona",
            info="Matches Customer Support / Finance / Executive / Platform modes.",
        )
        chatbot = gr.Chatbot(height=420, label="Conversation")
        msg = gr.Textbox(
            placeholder="Type your message here...",
            label="Message",
            lines=1,
        )
        with gr.Row():
            send = gr.Button("Send", variant="primary")
            clear = gr.Button("Clear")

        def do_send(u, h, p):
            return _reply(u, h, p)

        send.click(do_send, [msg, chatbot, persona], [chatbot, msg])
        msg.submit(do_send, [msg, chatbot, persona], [chatbot, msg])
        clear.click(lambda: ([], ""), outputs=[chatbot, msg])

    return demo


if __name__ == "__main__":
    demo = build_ui()
    port = int(
        os.environ.get(
            "GRADIO_SERVER_PORT",
            os.environ.get("PORT", os.environ.get("DATABRICKS_APP_PORT", "7860")),
        )
    )
    host = os.environ.get("GRADIO_SERVER_NAME", "0.0.0.0")
    local = os.environ.get("DATABRICKS_APP_NAME") is None
    demo.launch(server_name=host, server_port=port, show_error=True, share=False, debug=local)
