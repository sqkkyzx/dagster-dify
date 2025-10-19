import io
import json
import os
from typing import List, Dict, Literal, Union, Optional

import httpx
from dagster import ConfigurableResource, OpExecutionContext, AssetExecutionContext, ExpectationResult
from pydantic import Field, BaseModel


class FileInput(BaseModel):
    type: Literal["document", "image", "audio", "video", "custom"]
    transfer_method: Literal["remote_url", "local_file"]
    url: Optional[str] = None
    upload_file_id: Optional[str] = None

FileInputs = Union[FileInput, List[FileInput], Dict[str, str], List[Dict[str, str]]]


class DifyResource(ConfigurableResource):
    """
[Dify API](https://docs.dify.ai/zh-hans/guides/application-publishing/developing-with-apis)
    """

    BaseURL: str = Field(description="Example: https://<your-domain>/v1")

    def dify_workflow_stream(
            self,
            token: str,
            inputs: Dict[str, str|FileInputs],
            user: str = "dagster",
            trace_id: str = None,
            context: OpExecutionContext | AssetExecutionContext = None,
            timeout: int = 30000,
    ) -> Dict:
        llm_workflow_output: Dict|None = None

        app_info = self.app_info(token)

        headers = {'Authorization': f'Bearer {token}',}
        if trace_id:
            headers['X-Trace-Id'] = trace_id

        with httpx.stream(
            method="POST",
            url=f"{self.BaseURL}/workflows/run",
            headers=headers,
            json={"inputs": inputs, "response_mode": "streaming", "user": user},
            follow_redirects=True,
            timeout=timeout,
        ) as response:
            node_cache = {}
            for line in response.iter_lines():
                if not line.startswith("data:"):
                    continue

                event_data = json.loads(line[len("data:"):].strip())
                event = event_data.get('event')

                task_id = event_data.get('task_id')
                workflow_run_id = event_data.get('workflow_run_id')
                data = event_data.get('data', {})
                status = data.get('status')
                error = data.get('error')

                node_index = data.get('index')

                log_label = f"DIFY_{event}".upper()
                log_metadata = {"task_id": task_id, "workflow_run_id": workflow_run_id}

                if event == "workflow_started":
                    context.log_event(ExpectationResult(
                        success=True,
                        label=log_label,
                        description=F"{log_label}: Task Started.",
                        metadata=log_metadata | app_info.model_dump(),
                    ))
                elif event == "node_started":
                    node_cache[str(node_index)] = data.get('title')
                    context.log_event(ExpectationResult(
                        success=True,
                        label=F"{log_label}: Node{node_index} [{data.get('title')}] started.",
                        description=F"{log_label}: Task Started.",
                        metadata=log_metadata | {
                            "node_title": data.get('title'),
                            "node_id": data.get('node_id'),
                            "node_type": data.get('node_type'),
                            "node_index": node_index,
                            "predecessor_node_id": data.get('predecessor_node_id'),
                            "inputs": data.get('inputs'),
                        },
                    ))
                elif event == "node_finished":
                    context.log_event(ExpectationResult(
                        success=True if status in ["succeeded", "running"] else False,
                        label=log_label,
                        description=F"{log_label}: Task {task_id} node{node_index} {status}.",
                        metadata=log_metadata | {
                            "node_title": node_cache.get(str(node_index)),
                            "error": error,
                            "elapsed_time": data.get('elapsed_time'),
                        } | (data.get('execution_metadata', {}) or {}) | {
                            "node_id": data.get('node_id'),
                            "node_index": node_index,
                            "predecessor_node_id": data.get('predecessor_node_id'),
                            "outputs": data.get('outputs'),
                        },
                    ))
                elif event == "workflow_finished":
                    context.log_event(ExpectationResult(
                        success=True if status in ["succeeded", "running"] else False,
                        label=log_label,
                        description=F"{log_label}: Task {status}.",
                        metadata=log_metadata | {
                            "error": data.get('error'),
                            "elapsed_time": data.get('elapsed_time'),
                            "total_tokens": data.get('total_tokens'),
                            "total_steps": data.get('total_steps'),
                            "outputs": data.get('outputs'),
                        },
                    ))

                    llm_workflow_output = data.get("outputs", {})
                else:
                    continue

        return llm_workflow_output


    def upload_file(
            self,
            token: str,
            file_name: Optional[str],
            file_path: Optional[str] = None,
            file_bytes: Optional[bytes] = None,
            user: str = "dagster",
            timeout: int = 300000
    ):
        if not file_name:
            file_name = os.path.basename(file_path)

        if file_bytes is not None:
            with io.BytesIO(file_bytes) as file:
                res = httpx.post(
                    f"{self.BaseURL}/files/upload",
                    files={"file": (file_name, file)},
                    json={"user": user},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=timeout
                ).json()
                return res
        elif file_path is not None:
            with open(file_path, "rb") as file:
                res = httpx.post(
                    f"{self.BaseURL}/files/upload",
                    files={"file": (file_name, file)},
                    json={"user": user},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=timeout
                ).json()
                return res
        else:
            raise "At least one of file_path or file_bytes should be provided."

    def app_info(self, token: str):
        res = httpx.get(
            f"{self.BaseURL}/info",
            headers={"Authorization": f"Bearer {token}"},
        ).json()
        return AppInfo(**res)

    def webapp_info(self, token: str):
        res = httpx.get(
            f"{self.BaseURL}/site",
            headers={"Authorization": f"Bearer {token}"},
        ).json()
        return WebAppInfo(**res)


class AppInfo(BaseModel):
    name: str
    description: str
    tags: List[str]
    mode: str
    author_name: str


class WebAppInfo(BaseModel):
    title: str
    icon_type: str
    icon: str
    icon_background: str
    icon_url: str
    description: str
    copyright: str
    privacy_policy: str
    custom_disclaimer: str
    default_language: str
    show_workflow_steps: str
