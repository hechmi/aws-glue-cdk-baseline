# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from typing import Dict
from os import path
from aws_cdk import (
    Stack,
    aws_glue_alpha as glue,
    aws_iam as iam,
)
from constructs import Construct

class GlueAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, config:Dict, stage:str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create cross-account role
        self.cross_account_role = self.create_cross_account_role(
            f"GlueCrossAccountRole-{stage}",
            str(config['pipelineAccount']['awsAccountId'])
        )

        # For integration test
        self.iam_role = iam.Role(self, "GlueTestRole",
            role_name=f"glue-test-{stage}",
            assumed_by=iam.ArnPrincipal(f"arn:aws:iam::{str(config['pipelineAccount']['awsAccountId'])}:root"),
            inline_policies={
                "GluePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "glue:GetJobs",
                                "glue:GetJobRun",
                                "glue:GetTags",
                                "glue:StartJobRun"
                            ],
                            resources=[
                                "*"
                            ]
                        )
                    ]
                )
            }
        )
    def create_cross_account_role(self, role_name: str, trusted_account_id: str):
        return iam.Role(self, f"{role_name}CrossAccountRole",
            role_name=role_name,
            assumed_by=iam.AccountPrincipal(trusted_account_id),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")]
        )
    @property
    def iam_role_arn(self):
        return self.iam_role.role_arn
    @property
    def cross_account_role_arn(self):
        return self.glue_app_stack.cross_account_role_arn