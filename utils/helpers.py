import yaml

def load_cfg(cfg_file):
    """
    Load configuration from a YAML config file
    """
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    return cfg

def create_bucket_if_not_exists(s3_client, bucket_name):
    """
    Check if bucket exists.
    If not (404 error) -> Create new.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' not found. Creating new...")
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Successfully created bucket: {bucket_name}")
            except ClientError as ce:
                print(f"Error creating bucket: {ce}")
                return False
        else:
            print(f"Unknown error checking bucket: {e}")
            return False
    return True