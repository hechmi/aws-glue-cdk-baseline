from typing import Dict
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_codebuild as codebuild
)
from constructs import Construct
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, CodeBuildStep
from aws_glue_cdk_baseline.glue_app_stage import GlueAppStage
 
GITHUB_REPO = "hechmi/aws-glue-cdk-baseline"
GITHUB_BRANCH = "main"
GITHUB_CONNECTION_ARN = "arn:aws:codeconnections:us-east-1:009507777973:connection/d768c13d-4e9f-499a-be0a-52644fa2ad44"
 
class PipelineStack(Stack):
 
    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
 
        source = CodePipelineSource.connection(
            GITHUB_REPO,
            GITHUB_BRANCH,
            connection_arn=GITHUB_CONNECTION_ARN
        )
 
        pipeline = CodePipeline(self, "GluePipeline",
            pipeline_name="GluePipeline",
            cross_account_keys=True,
            docker_enabled_for_synth=True,
            synth=CodeBuildStep("CdkSynth",
                input=source,
                install_commands=[
                    "pip install -r requirements.txt",
                    "pip install -r requirements-dev.txt",
                    "npm install -g aws-cdk",
                ],
                commands=[
                    "cdk synth",
                ],
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_7_0
                )
            )
        )
 
        # Add development stage
        dev_stage = GlueAppStage(self, "DevStage", config=config, stage="dev", 
            env=cdk.Environment(
                account=str(config['devAccount']['awsAccountId']),
                region=config['devAccount']['awsRegion']
            ))
        pipeline.add_stage(dev_stage)

        # Add production stage
        prod_stage = GlueAppStage(self, "ProdStage", config=config, stage="prod", 
            env=cdk.Environment(
                account=str(config['prodAccount']['awsAccountId']),
                region=config['prodAccount']['awsRegion']
            ))
        pipeline.add_stage(prod_stage)
 
        # Glue Resource Sync as a separate step in the pipeline
        pipeline.add_wave("GlueJobSync").add_post(CodeBuildStep("GlueJobSync",
            input=source,
            commands=[
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                "python aws_glue_cdk_baseline/job_scripts/sync.py "
                   "--dst-role-arn arn:aws:iam::{0}:role/GlueCrossAccountRole-prod "
                   "--dst-region {1} "
                   "--deserialize-from-file aws_glue_cdk_baseline/resources/resources.json "
                   "--config-path mapping.json "
                   "--targets job,catalog "
                   "--skip-prompt".format(
                       config['prodAccount']['awsAccountId'],
                       config['prodAccount']['awsRegion']
                   ),
            ],
            role_policy_statements=[
                iam.PolicyStatement(
                    actions=[
                        "sts:AssumeRole",
                    ],
                    resources=["*"]
                )
            ],
            build_environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0
            )
        ))