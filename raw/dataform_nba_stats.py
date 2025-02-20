import time
from pathlib import Path
from google.cloud import dataform_v1beta1
from raw.settings.config import settings
import logging

logger = logging.getLogger(__name__)

def run_dataform_nba_stats():

    client = dataform_v1beta1.DataformClient()

    BQ_PROJECT = settings.BQ_PROJECT
    SCRIPT = Path(__file__).stem
    LOCATION = 'southamerica-east1'
    GIT_REPO = 'nba_dataform'
    WORKFLOW = 'workflow_run_main'

    logger.info({"message": f"Starting {SCRIPT} workflow.."})

    repo_uri = f'projects/{BQ_PROJECT}/locations/{LOCATION}/repositories/{GIT_REPO}'
    workflow_config = f'projects/{BQ_PROJECT}/locations/{LOCATION}/repositories/{GIT_REPO}/workflowConfigs/{WORKFLOW}'

    request = dataform_v1beta1.CreateWorkflowInvocationRequest(
        parent=repo_uri,
        workflow_invocation=dataform_v1beta1.types.WorkflowInvocation(
            workflow_config=workflow_config
        )
    )

    response = client.create_workflow_invocation(request=request)
    workflow_invocation_name = response.name

    while True:

        workflow_details = client.get_workflow_invocation(name=workflow_invocation_name)
        status = workflow_details.state
        logger.info({"message": f"Workflow invocation status: {status}"})

        if status == dataform_v1beta1.types.WorkflowInvocation.State.SUCCEEDED:
            logger.info({"message": "Workflow succeeded!"})
            break
        elif status == dataform_v1beta1.types.WorkflowInvocation.State.FAILED:
            raise RuntimeError("Workflow failed!")
        else:
            time.sleep(5)


if __name__=="__main__":
    run_dataform_nba_stats()