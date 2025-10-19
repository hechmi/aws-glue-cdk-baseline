import yaml
import json
 
def generate_mapping():
    with open('default-config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    mapping = {
        f"s3://aws-glue-assets-{config['devAccount']['awsAccountId']}-{config['devAccount']['awsRegion']}": f"s3://aws-glue-assets-{config['prodAccount']['awsAccountId']}-{config['prodAccount']['awsRegion']}",
        f"arn:aws:iam::{config['devAccount']['awsAccountId']}:role/service-role/AWSGlueServiceRole": f"arn:aws:iam::{config['prodAccount']['awsAccountId']}:role/service-role/AWSGlueServiceRole",
        f"s3://dev-glue-data-{config['devAccount']['awsAccountId']}-{config['prodAccount']['awsRegion']}": f"s3://prod-glue-data-{config['prodAccount']['awsAccountId']}-{config['prodAccount']['awsRegion']}"
    }
    with open('mapping.json', 'w') as mapping_file:
        json.dump(mapping, mapping_file, indent=2)
 
if __name__ == "__main__":
    generate_mapping()
 