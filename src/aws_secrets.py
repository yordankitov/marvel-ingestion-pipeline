"""
Module for single-source-of-truth settings management through
the usage of Pydantic BaseSettings and AWS Secrets Manager.
"""
import json
import logging

import boto3
from botocore.exceptions import ClientError
from pydantic.types import SecretStr
from pydantic_settings import BaseSettings

if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
        level=logging.INFO,
    )


class AWSSecretsManagerSettings(BaseSettings):
    """
    This class is created on the base of pydantic's BaseSettings module. It is a representation
    of the secret values used for authenticating the AuroraDB and Snowflake access. The purpose of
    this class is to ease the use of accessing the secrets, and increase the security.
    """

    USER: SecretStr
    PASSWORD: SecretStr
    DATABASE: str
    ACCOUNT: SecretStr
    SCHEMA: str
    ROLE: str
    WAREHOUSE: str

    class Config:
        """
        The config class is part of pydantic's way of adding more configuration
        to the settings that have been set as part of the BaseSettings class.

        Explanation for each configuration setting is below:
        case_sensitive - passed values need to match the name of the specified attributes
        allow_mutation - is changing of the class's structure allowed during runtime
        extra - if additional attributes other than the ones specified within the class are allowed.
        """

        case_sensitive = True
        frozen = False
        extra = "ignore"


def get_aws_secret(secret_name: str, region_name: str = "eu-west-1"):
    """Function to obtain AWS Secrets using boto3 and the AWS Secrets Manager.
    Args:
        secret_name (str): the AWS name of the secret to retrieve
        region_name (str, optional): the AWS region. Defaults to "eu-west-1"
    Returns:
        secret (dict): dictionary with all secrets
    Raises:
        error: ResourceNotFoundException
        error: InvalidRequestException
        error: InvalidParameterException
        error: InternalServiceError
        error: DecryptionFailure

    """
    secret_name = secret_name
    secret_data = "{}"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )
    try:
        response = client.get_secret_value(SecretId=secret_name)

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            logging.critical("The requested secret " + secret_name + " was not found")
            raise error
        if error.response["Error"]["Code"] == "InvalidRequestException":
            logging.critical("The request was invalid due to:", error)
            raise error
        if error.response["Error"]["Code"] == "InvalidParameterException":
            logging.critical("The request had invalid params:", error)
            raise error
        if error.response["Error"]["Code"] == "DecryptionFailure":
            logging.critical("Requested secret can't be decrypted:", error)
            raise error
        if error.response["Error"]["Code"] == "InternalServiceError":
            logging.critical("An error occurred on service side:", error)
            raise error
    else:
        logging.info("Secrets obtained successfully!")
        # Secrets Manager decrypts the secret value using the associated KMS CMK.
        # Depending on whether the secret was a string or binary,
        # only one of these fields will be populated
        if "SecretString" in response:
            secret_data = response["SecretString"]
        else:
            secret_data = response["SecretBinary"]

    secret = json.loads(secret_data)
    return secret


def get_secret_values(secret_name: str):
    """Function to get and convert into dictionary the aws secret.
    Args:
        secret_name (str): the AWS name of the secret
    Returns:
        dict: dictionary with formatted secrets
    """
    secrets = AWSSecretsManagerSettings(**get_aws_secret(secret_name=secret_name))
    return {
        "user": secrets.USER.get_secret_value(),
        "password": secrets.PASSWORD.get_secret_value(),
        "database": secrets.DATABASE,
        "account": secrets.ACCOUNT.get_secret_value(),
        "schema": secrets.SCHEMA,
        "role": secrets.ROLE,
        "warehouse": secrets.WAREHOUSE,
    }
