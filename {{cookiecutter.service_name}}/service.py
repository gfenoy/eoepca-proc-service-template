# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
from __future__ import annotations
from typing import Dict
import pathlib

try:
    import zoo
except ImportError:
    # Use centralized ZooStub from zoo-runner-common package
    from zoo_runner_common.zoostub import ZooStub
    zoo = ZooStub()

import json
import os
import sys

import jwt
import requests
import yaml
from loguru import logger
from pystac import Catalog, Collection, read_file
from pystac.item_collection import ItemCollection
from pystac.stac_io import StacIO
from zoo_calrissian_runner import ZooCalrissianRunner, ExecutionHandler
from zoo_template_common import CustomStacIO

# For DEBUG
import traceback

logger.remove()
logger.add(sys.stderr, level="INFO")

StacIO.set_default(CustomStacIO)


class EoepcaCalrissianRunnerExecutionHandler(ExecutionHandler):
    """EOEPCA-specific execution handler with Workspace API integration."""

    def __init__(self, conf, outputs):
        super().__init__(conf=conf, outputs=outputs)
        self.http_proxy_env = os.environ.get("HTTP_PROXY", None)
        self.username = None
        self.feature_collection = None

        # Get auth environment
        auth_env = self.conf.get("auth_env", {})
        self.ades_rx_token = auth_env.get("jwt", "")

        # Get EOEPCA configuration
        eoepca = self.conf.get("eoepca", {})
        self.domain = eoepca.get("domain", "")
        self.workspace_url = eoepca.get("workspace_url", "")
        self.workspace_prefix = eoepca.get("workspace_prefix", "")

        # Should the user's Workspace bucket be used for stage-out?
        if self.workspace_url and self.workspace_prefix:
            self.use_workspace = True
        else:
            self.use_workspace = False

        # Should outputs be registered to the Workspace Catalogue?
        self.workspace_catalog_register = self.use_workspace and (
            (eoepca.get("workspace_catalog_register", "false")).lower() == "true"
        )

        # Initialize config defaults
        self.init_config_defaults(self.conf)

    def unset_http_proxy_env(self):
        """Temporarily unset HTTP_PROXY environment variable."""
        http_proxy = os.environ.pop("HTTP_PROXY", None)
        logger.info(f"Unsetting env HTTP_PROXY, whose value was {http_proxy}")

    def restore_http_proxy_env(self):
        """Restore HTTP_PROXY environment variable if it was set."""
        if self.http_proxy_env:
            os.environ["HTTP_PROXY"] = self.http_proxy_env
            logger.info(f"Restoring env HTTP_PROXY, to value {self.http_proxy_env}")

    @staticmethod
    def get_user_name(decodedJwt):
        """Extract username from JWT token. Supports multiple username fields."""
        for key in ["username", "user_name", "preferred_username"]:
            if key in decodedJwt:
                return decodedJwt[key]
        return None

    @staticmethod
    def init_config_defaults(conf):
        """Initialize default configuration parameters for stage-in/stage-out."""
        if "additional_parameters" not in conf:
            conf["additional_parameters"] = {}

        # Stage-in defaults
        conf["additional_parameters"]["STAGEIN_AWS_SERVICEURL"] = os.environ.get(
            "STAGEIN_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000"
        )
        conf["additional_parameters"]["STAGEIN_AWS_ACCESS_KEY_ID"] = os.environ.get(
            "STAGEIN_AWS_ACCESS_KEY_ID", "minio-admin"
        )
        conf["additional_parameters"]["STAGEIN_AWS_SECRET_ACCESS_KEY"] = os.environ.get(
            "STAGEIN_AWS_SECRET_ACCESS_KEY", "minio-secret-password"
        )
        conf["additional_parameters"]["STAGEIN_AWS_REGION"] = os.environ.get(
            "STAGEIN_AWS_REGION", "RegionOne"
        )

        # Stage-out defaults
        conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = os.environ.get(
            "STAGEOUT_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000"
        )
        conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = os.environ.get(
            "STAGEOUT_AWS_ACCESS_KEY_ID", "minio-admin"
        )
        conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = (
            os.environ.get("STAGEOUT_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        )
        conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = os.environ.get(
            "STAGEOUT_AWS_REGION", "RegionOne"
        )
        conf["additional_parameters"]["STAGEOUT_OUTPUT"] = os.environ.get(
            "STAGEOUT_OUTPUT", "eoepca"
        )

    def pre_execution_hook(self):
        """Hook to run before execution with EOEPCA Workspace integration."""
        try:
            logger.info("Pre execution hook")
            self.unset_http_proxy_env()

            # Decode JWT token to get username
            username_source = None
            if self.ades_rx_token:
                self.username = self.get_user_name(
                    jwt.decode(self.ades_rx_token, options={"verify_signature": False})
                )
                if self.username:
                    username_source = "JWT"

            # Fallback: get username from Path-Prefix env var
            if not self.username:
                self.username = os.getenv("SERVICES_NAMESPACE")
                if self.username:
                    username_source = "Path-Prefix"

            # Log username outcome
            if self.username:
                logger.info(f"Using username {self.username} from {username_source}")
            else:
                logger.warning("Unable to determine username")

            # Lookup workspace storage details if configured
            if self.use_workspace:
                logger.info("Lookup storage details in Workspace")

                uri_for_request = f"workspaces/{self.workspace_prefix}-{self.username}"
                workspace_api_endpoint = os.path.join(
                    self.workspace_url, uri_for_request
                )
                logger.info(f"Using Workspace API endpoint {workspace_api_endpoint}")

                headers = {"accept": "application/json"}
                if self.ades_rx_token:
                    headers["Authorization"] = f"Bearer {self.ades_rx_token}"

                get_workspace_details_response = requests.get(
                    workspace_api_endpoint, headers=headers
                )

                # GOOD response from Workspace API - use the details
                if get_workspace_details_response.ok:
                    workspace_response = get_workspace_details_response.json()
                    logger.info("Set user bucket settings")

                    storage_credentials = workspace_response["storage"]["credentials"]

                    self.conf["additional_parameters"][
                        "STAGEOUT_AWS_SERVICEURL"
                    ] = storage_credentials.get("endpoint")
                    self.conf["additional_parameters"][
                        "STAGEOUT_AWS_ACCESS_KEY_ID"
                    ] = storage_credentials.get("access")
                    self.conf["additional_parameters"][
                        "STAGEOUT_AWS_SECRET_ACCESS_KEY"
                    ] = storage_credentials.get("secret")
                    self.conf["additional_parameters"][
                        "STAGEOUT_AWS_REGION"
                    ] = storage_credentials.get("region")
                    self.conf["additional_parameters"][
                        "STAGEOUT_OUTPUT"
                    ] = storage_credentials.get("bucketname")
                # BAD response from Workspace API - fallback to pre-configured storage
                else:
                    logger.error("Problem connecting with the Workspace API")
                    logger.info(
                        f"  Response code = {get_workspace_details_response.status_code}"
                    )
                    logger.info(
                        f"  Response text = \n{get_workspace_details_response.text}"
                    )
                    self.use_workspace = False
                    logger.info("Using pre-configured storage details")
            else:
                logger.info("Using pre-configured storage details")

            lenv = self.conf.get("lenv", {})
            self.conf["additional_parameters"]["collection_id"] = lenv.get("usid", "")
            self.conf["additional_parameters"]["process"] = os.path.join(
                "processing-results",
                self.conf["additional_parameters"]["collection_id"],
            )

        except Exception as e:
            logger.error("ERROR in pre_execution_hook...")
            logger.error(traceback.format_exc())
            raise (e)

        finally:
            self.restore_http_proxy_env()

    def post_execution_hook(self, log, output, usage_report, tool_logs):
        """Hook to run after execution with EOEPCA STAC catalog registration."""
        try:
            logger.info("Post execution hook")
            self.unset_http_proxy_env()

            logger.info("Set user bucket settings")
            os.environ["AWS_S3_ENDPOINT"] = self.conf["additional_parameters"][
                "STAGEOUT_AWS_SERVICEURL"
            ]
            os.environ["AWS_ACCESS_KEY_ID"] = self.conf["additional_parameters"][
                "STAGEOUT_AWS_ACCESS_KEY_ID"
            ]
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.conf["additional_parameters"][
                "STAGEOUT_AWS_SECRET_ACCESS_KEY"
            ]
            os.environ["AWS_REGION"] = self.conf["additional_parameters"][
                "STAGEOUT_AWS_REGION"
            ]

            StacIO.set_default(CustomStacIO)

            for i in self.outputs:
                logger.info(f"Output {i}: {self.outputs[i]}")
                if "mimeType" in self.outputs[i]:
                    self.setOutput(i, output)
                else:
                    logger.warning(f"Output {i} has no mimeType, skipping...")
                    self.outputs[i]["value"] = str(output[i])

        except Exception as e:
            logger.error("ERROR in post_execution_hook...")
            logger.error(traceback.format_exc())
            raise (e)

        finally:
            self.restore_http_proxy_env()

    def setOutput(self, outputName, values):
        """Process and set output values from STAC catalog with EOEPCA registration."""
        output = self.outputs[outputName]
        logger.info(f"Read catalog from STAC Catalog URI: {output} -> {values}")

        if not isinstance(values[outputName], list):
            logger.info(
                f"values[{outputName}] is not a list, transform to an array"
            )
            values[outputName] = [values[outputName]]

        items = []

        for i in range(len(values[outputName])):
            if values[outputName][i] is None:
                break
            s3_path = values[outputName][i]["value"]
            try:
                if s3_path.count("s3://") == 0:
                    s3_path = "s3://" + s3_path
                cat: Catalog = read_file(s3_path)
            except Exception as e:
                logger.error(f"No collection found in the output catalog {e}")
                output["collection"] = json.dumps({}, indent=2)
                return

            collection_id = self.conf["additional_parameters"]["collection_id"]
            logger.info(f"Create collection with ID {collection_id}")

            collection = None

            try:
                logger.info(f"Catalog : {dir(cat)}")
                collection: Collection = next(cat.get_all_collections())
            except Exception as e:
                try:
                    items_from_cat = cat.get_all_items()
                    itemFinal = []
                    for item in items_from_cat:
                        for a in item.assets.keys():
                            cDict = item.assets[a].to_dict()
                            cDict["storage:platform"] = "EOEPCA"
                            cDict["storage:requester_pays"] = False
                            cDict["storage:tier"] = "Standard"
                            cDict["storage:region"] = self.conf[
                                "additional_parameters"
                            ]["STAGEOUT_AWS_REGION"]
                            cDict["storage:endpoint"] = self.conf[
                                "additional_parameters"
                            ]["STAGEOUT_AWS_SERVICEURL"]
                            item.assets[a] = item.assets[a].from_dict(cDict)
                        item.collection_id = collection_id
                        itemFinal += [item.clone()]
                        items.append(item.clone())
                    collection = ItemCollection(items=itemFinal)
                    logger.info("Created collection from items")
                except Exception as e:
                    logger.error(
                        f"No collection or item found in the output catalog {e}"
                    )

        # Trap the case of no output collection
        if collection is None:
            logger.error("ABORT: The output collection is empty")
            output["collection"] = json.dumps({}, indent=2)
            return

        if len(items) > 0:
            collection = ItemCollection(items=itemFinal)
        collection_dict = collection.to_dict()
        collection_dict["id"] = collection_id
        output["collection"] = collection_dict
        output["collection"]["id"] = collection_id

        # Register with the workspace catalogue if configured
        if self.workspace_catalog_register:
            logger.info(
                f"Register collection in workspace {self.workspace_prefix}-{self.username}"
            )
            headers = {"Accept": "application/json"}
            if self.ades_rx_token:
                headers["Authorization"] = f"Bearer {self.ades_rx_token}"
            api_endpoint = f"{self.workspace_url}/workspaces/{self.workspace_prefix}-{self.username}"
            r = requests.post(
                f"{api_endpoint}/register-json",
                json=collection_dict,
                headers=headers,
            )
            logger.info(f"Register collection response: {r.status_code}")

            logger.info(f"Register processing results to collection")
            r = requests.post(
                f"{api_endpoint}/register",
                json={"type": "stac-item", "url": collection.get_self_href()},
                headers=headers,
            )
            logger.info(f"Register processing results response: {r.status_code}")


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
            conf["service_logs"][keys[0]]=os.path.join(conf['main']['tmpUrl'],
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
