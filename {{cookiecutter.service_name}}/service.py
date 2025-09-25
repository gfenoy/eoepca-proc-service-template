# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
from __future__ import annotations
from typing import Dict
import pathlib

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()

import json
import os
import sys
from urllib.parse import urlparse

import boto3  # noqa: F401
import botocore
import jwt
import requests
import yaml
from botocore.exceptions import ClientError
from loguru import logger
from pystac import read_file, Collection, Catalog
from pystac.stac_io import DefaultStacIO, StacIO
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner
from botocore.client import Config
from pystac.item_collection import ItemCollection

# For DEBUG
import traceback

logger.remove()
logger.add(sys.stderr, level="INFO")


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()
        self.s3_client = self.session.create_client(
            service_name="s3",
            region_name=os.environ.get("AWS_REGION"),
            endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            verify=True,
            use_ssl=True,
            config=Config(s3={"addressing_style": "path", "signature_version": "s3v4"}),
        )

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            return (
                self.s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=parsed.netloc,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        else:
            super().write_text(dest, txt, *args, **kwargs)


StacIO.set_default(CustomStacIO)


class EoepcaCalrissianRunnerExecutionHandler(ExecutionHandler):
    def __init__(self, conf, outputs):
        super().__init__()
        self.conf = conf
        self.outputs = outputs

        self.http_proxy_env = os.environ.get("HTTP_PROXY", None)

        eoepca = self.conf.get("eoepca", {})
        self.domain = eoepca.get("domain", "")
        self.workspace_url = eoepca.get("workspace_url", "")
        self.workspace_prefix = eoepca.get("workspace_prefix", "")

        # Should the user's Workspace bucket be used for stage-out?
        # Only if both the workspace url, and the workspace prefix have been specified.
        if self.workspace_url and self.workspace_prefix:
            self.use_workspace = True
        else:
            self.use_workspace = False

        # Should outputs be registered to the Workspace Catalogue?
        # Only if we are using the Workspace, and catalogue registration has been specified.
        self.workspace_catalog_register = self.use_workspace and ((eoepca.get("workspace_catalog_register", "false")).lower() == "true")

        self.username = None
        auth_env = self.conf.get("auth_env", {})
        self.ades_rx_token = auth_env.get("jwt", "")

        self.feature_collection = None

        self.init_config_defaults(self.conf)

    def pre_execution_hook(self):
        try:
            logger.info("Pre execution hook")
            self.unset_http_proxy_env()

            # DEBUG
            # logger.info(f"zzz PRE-HOOK - config...\n{json.dumps(self.conf, indent=2)}\n")
            
            # decode the JWT token to get the user name
            username_source = None
            if self.ades_rx_token:
                self.username = self.get_user_name(
                    jwt.decode(self.ades_rx_token, options={"verify_signature": False})
                )
                if self.username:
                    username_source = "JWT"

            # Else get username from Path-Prefix - already parsed into env var
            if not self.username:
                self.username = os.getenv("SERVICES_NAMESPACE")
                if self.username:
                    username_source = "Path-Prefix"

            # Log username outcome
            if self.username:
                logger.info(f"Using username {self.username} from {username_source}")
            else:
                logger.warning("Unable to determine username")

            if self.use_workspace:
                logger.info("Lookup storage details in Workspace")

                # Workspace API endpoint
                uri_for_request = f"workspaces/{self.workspace_prefix}-{self.username}"

                workspace_api_endpoint = os.path.join(self.workspace_url, uri_for_request)
                logger.info(f"Using Workspace API endpoint {workspace_api_endpoint}")

                # Request: Get Workspace Details
                headers = {
                    "accept": "application/json",
                }
                if self.ades_rx_token:
                    headers["Authorization"] = f"Bearer {self.ades_rx_token}"
                get_workspace_details_response = requests.get(workspace_api_endpoint, headers=headers)

                # GOOD response from Workspace API - use the details
                if get_workspace_details_response.ok:
                    workspace_response = get_workspace_details_response.json()

                    logger.info("Set user bucket settings")

                    storage_credentials = workspace_response["storage"]["credentials"]

                    self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = storage_credentials.get("endpoint")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = storage_credentials.get("access")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = storage_credentials.get("secret")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = storage_credentials.get("region")
                    self.conf["additional_parameters"]["STAGEOUT_OUTPUT"] = storage_credentials.get("bucketname")
                # BAD response from Workspace API - fallback to the 'pre-configured storage details'
                else:
                    logger.error("Problem connecting with the Workspace API")
                    logger.info(f"  Response code = {get_workspace_details_response.status_code}")
                    logger.info(f"  Response text = \n{get_workspace_details_response.text}")
                    self.use_workspace = False
                    logger.info("Using pre-configured storage details")
            else:
                logger.info("Using pre-configured storage details")

            lenv = self.conf.get("lenv", {})
            self.conf["additional_parameters"]["collection_id"] = lenv.get("usid", "")
            self.conf["additional_parameters"]["process"] = os.path.join("processing-results", self.conf["additional_parameters"]["collection_id"])

        except Exception as e:
            logger.error("ERROR in pre_execution_hook...")
            logger.error(traceback.format_exc())
            raise(e)
        
        finally:
            self.restore_http_proxy_env()


    def post_execution_hook(self, log, output, usage_report, tool_logs):
        try:
            logger.info("Post execution hook")
            self.unset_http_proxy_env()

            # DEBUG
            # logger.info(f"zzz POST-HOOK - config...\n{json.dumps(self.conf, indent=2)}\n")

            logger.info("Set user bucket settings")
            os.environ["AWS_S3_ENDPOINT"] = self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"]
            os.environ["AWS_ACCESS_KEY_ID"] = self.conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"]
            os.environ["AWS_REGION"] = self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"]

            StacIO.set_default(CustomStacIO)
            for i in self.outputs:
                logger.info(f"Output {i}: {self.outputs[i]}")
                if "mimeType" in self.outputs[i]:
                    self.setOutput(i,output)
                else:
                    logger.warning(f"Output {i} has no mimeType, skipping...")
                    self.outputs[i]["value"] = str(output[i])

        except Exception as e:
            logger.error("ERROR in post_execution_hook...")
            logger.error(traceback.format_exc())
            raise(e)

        finally:
            self.restore_http_proxy_env()

    def setOutput(self, outputName, values):
        output=self.outputs[outputName]
        logger.info(f"Read catalog from STAC Catalog URI: {output} -> {values}")
        #logger.info(f"Read catalog => STAC Catalog URI: {output['StacCatalogUri']}")
        if not(isinstance(values[outputName], list)):
            logger.info(f"values[{outputName}] is not a list, tranform to an array")
            values[outputName]=[values[outputName]]

        items = []

        for i in range(len(values[outputName])):
            if values[outputName][i] is None:
                break
            s3_path = values[outputName][i]["value"]
            try:
                if s3_path.count("s3://")==0:
                    s3_path = "s3://" + s3_path
                cat: Catalog  = read_file(s3_path)
            except Exception as e:
                logger.error(f"No collection found in the output catalog {e}")
                output["collection"] = json.dumps({}, indent=2)
                return

            collection_id = collection_id = self.conf["additional_parameters"]["collection_id"]

            logger.info(f"Create collection with ID {collection_id}")

            collection = None

            try:
                logger.info(f"Catalog : {dir(cat)}")
                collection: Collection = next(cat.get_all_collections())
            except Exception as e:
                try:
                    items=cat.get_all_items()
                    itemFinal=[]
                    for i in items:
                        for a in i.assets.keys():
                            cDict=i.assets[a].to_dict()
                            cDict["storage:platform"]="EOEPCA"
                            cDict["storage:requester_pays"]=False
                            cDict["storage:tier"]="Standard"
                            cDict["storage:region"]=self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"]
                            cDict["storage:endpoint"]=self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"]
                            i.assets[a]=i.assets[a].from_dict(cDict)
                        i.collection_id=collection_id
                        itemFinal+=[i.clone()]
                        items.append(i.clone())
                    collection = ItemCollection(items=itemFinal)
                    logger.info("Created collection from items")
                except Exception as e:
                    logger.error(f"No collection or item found in the output catalog {e}")
            
        # Trap the case of no output collection
        if collection is None:
            logger.error("ABORT: The output collection is empty")
            output["collection"] = json.dumps({}, indent=2)
            return

        if len(items)>0:
            collection = ItemCollection(items=itemFinal)
        collection_dict=collection.to_dict()
        collection_dict["id"]=collection_id
        output["collection"] = collection_dict
        output["collection"]["id"] = collection_id

        # Register with the workspace catalogue
        if self.workspace_catalog_register:
            logger.info(f"Register collection in workspace {self.workspace_prefix}-{self.username}")
            headers = {
                "Accept": "application/json",
            }
            if self.ades_rx_token:
                headers["Authorization"] = f"Bearer {self.ades_rx_token}"
            api_endpoint = f"{self.workspace_url}/workspaces/{self.workspace_prefix}-{self.username}"
            r = requests.post(
                f"{api_endpoint}/register-json",
                json=collection_dict,
                headers=headers,
            )
            logger.info(f"Register collection response: {r.status_code}")

            # TODO pool the catalog until the collection is available
            #self.feature_collection = requests.get(
            #    f"{api_endpoint}/collections/{collection.id}", headers=headers
            #).json()
        
            logger.info(f"Register processing results to collection")
            r = requests.post(f"{api_endpoint}/register",
                            json={"type": "stac-item", "url": collection.get_self_href()},
                            headers=headers,)
            logger.info(f"Register processing results response: {r.status_code}")

    def unset_http_proxy_env(self):
        http_proxy = os.environ.pop("HTTP_PROXY", None)
        logger.info(f"Unsetting env HTTP_PROXY, whose value was {http_proxy}")

    def restore_http_proxy_env(self):
        if self.http_proxy_env:
            os.environ["HTTP_PROXY"] = self.http_proxy_env
            logger.info(f"Restoring env HTTP_PROXY, to value {self.http_proxy_env}")

    @staticmethod
    def init_config_defaults(conf):
        if "additional_parameters" not in conf:
            conf["additional_parameters"] = {}

        conf["additional_parameters"]["STAGEIN_AWS_SERVICEURL"] = os.environ.get("STAGEIN_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000")
        conf["additional_parameters"]["STAGEIN_AWS_ACCESS_KEY_ID"] = os.environ.get("STAGEIN_AWS_ACCESS_KEY_ID", "minio-admin")
        conf["additional_parameters"]["STAGEIN_AWS_SECRET_ACCESS_KEY"] = os.environ.get("STAGEIN_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        conf["additional_parameters"]["STAGEIN_AWS_REGION"] = os.environ.get("STAGEIN_AWS_REGION", "RegionOne")

        conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = os.environ.get("STAGEOUT_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000")
        conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = os.environ.get("STAGEOUT_AWS_ACCESS_KEY_ID", "minio-admin")
        conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = os.environ.get("STAGEOUT_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = os.environ.get("STAGEOUT_AWS_REGION", "RegionOne")
        conf["additional_parameters"]["STAGEOUT_OUTPUT"] = os.environ.get("STAGEOUT_OUTPUT", "eoepca")

        # DEBUG
        # logger.info(f"init_config_defaults: additional_parameters...\n{json.dumps(conf['additional_parameters'], indent=2)}\n")

    @staticmethod
    def get_user_name(decodedJwt):
        for key in ["username", "user_name", "preferred_username"]:
            if key in decodedJwt:
                return decodedJwt[key]
        return None

    @staticmethod
    def local_get_file(fileName):
        """
        Read and load the contents of a yaml file

        :param yaml file to load
        """
        try:
            with open(fileName, "r") as file:
                data = yaml.safe_load(file)
            return data
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}

    def get_pod_env_vars(self):
        logger.info("get_pod_env_vars")

        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):
        logger.info("get_pod_node_selector")

        return self.conf.get("pod_node_selector", {})

    def get_secrets(self):
        logger.info("get_secrets")

        return self.local_get_file("/assets/pod_imagePullSecrets.yaml")

    def get_additional_parameters(self):
        logger.info("get_additional_parameters")

        return self.conf.get("additional_parameters", {})

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """
        try:
            logger.info("handle_outputs")

            # link element to add to the statusInfo
            self.conf['main']['tmpUrl']=self.conf['main']['tmpUrl'].replace("temp/",self.conf["auth_env"]["user"]+"/temp/")
            servicesLogs = [
                {
                    "url": os.path.join(self.conf['main']['tmpUrl'],
                                        f"{self.conf['lenv']['Identifier']}-{self.conf['lenv']['usid']}",
                                        os.path.basename(tool_log)),
                    "title": f"Tool log {os.path.basename(tool_log)}",
                    "rel": "related",
                }
                for tool_log in tool_logs
            ]
            cindex=0
            if "service_logs" in self.conf:
                cindex=1
            for i in range(len(servicesLogs)):
                okeys = ["url", "title", "rel"]
                keys = ["url", "title", "rel"]
                if cindex > 0:
                    for j in range(len(keys)):
                        keys[j] = keys[j] + "_" + str(cindex)
                if "service_logs" not in self.conf:
                    self.conf["service_logs"] = {}
                for j in range(len(keys)):
                    self.conf["service_logs"][keys[j]] = servicesLogs[i][okeys[j]]
                cindex += 1
            self.conf["service_logs"]["length"] = str(len(servicesLogs))

        except Exception as e:
            logger.error("ERROR in handle_outputs...")
            logger.error(traceback.format_exc())
            raise(e)


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs): # noqa

    try:
        with open(
            os.path.join(
                pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
                "app-package.cwl",
            ),
            "r",
        ) as stream:
            cwl = yaml.safe_load(stream)

        execution_handler = EoepcaCalrissianRunnerExecutionHandler(conf=conf, outputs=outputs)

        runner = ZooCalrissianRunner(
            cwl=cwl,
            conf=conf,
            inputs=inputs,
            outputs=outputs,
            execution_handler=execution_handler,
        )
        # DEBUG
        # runner.monitor_interval = 1

        # we are changing the working directory to store the outputs
        # in a directory dedicated to this execution
        working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
        os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(working_dir)

        exit_status = runner.execute()

        if exit_status == zoo.SERVICE_SUCCEEDED:
            logger.info(f"Setting Collection into output key {list(outputs.keys())[0]}")
            for i in outputs:
                logger.info(f"Setting Collection into output key {i}: {outputs[i]}")
                if "collection" in outputs[i]:
                    outputs[i]["value"] = json.dumps(
                        outputs[i]["collection"], indent=2
                    )
            return zoo.SERVICE_SUCCEEDED

        else:
            conf["lenv"]["message"] = zoo._("Execution failed")
            return zoo.SERVICE_FAILED

    except Exception as e:
        logger.error("ERROR in processing execution template...")
        try:
            with open(os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name(),"job.log"),"w",encoding="utf-8") as file:
                file.write(runner.execution.get_log())
            if "service_logs" not in conf:
                conf["service_logs"] = {}
            keys=["url","title","rel"]
            if "length" in conf["service_logs"]:
                for i in range(len(keys)):
                    keys[i]+="_"+str(int(conf["service_logs"]["length"]))
            conf["service_logs"][keys[0]]=os.path.join(conf['main']['tmpUrl'].replace("temp/",conf["auth_env"]["user"]+"/temp/"),
                    runner.get_namespace_name(),
                    "job.log")
            conf["service_logs"][keys[1]]="Job pod log"
            conf["service_logs"][keys[2]]="related"
            conf["service_logs"]["length"]="1"
            logger.info("Job log saved")
        except Exception as e:
            logger.error(f"{str(e)}")
        try:
            tool_logs = runner.execution.get_tool_logs()
            execution_handler.handle_outputs(None, None, None, tool_logs)
        except Exception as e:
            logger.error("Fethcing logs failed!"+str(e))
        stack = traceback.format_exc()
        logger.error(stack)
        conf["lenv"]["message"] = zoo._(f"Exception during execution...\n{stack}\n")
        return zoo.SERVICE_FAILED
