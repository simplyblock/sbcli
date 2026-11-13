"""The storage array of a multi-attach cluster (array LUN mode, phase 4).

simplyblock creates, presents, grows, fences and deletes the cluster's LUNs
through the array plugin registered here. Credentials go to the KMS
(cluster/<id>/array/<array id>) and are never returned; everything else is
kept on the cluster record. See simplyblock_core.controllers.array_controller.
"""

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field, SecretStr

from simplyblock_core.controllers import array_controller

from .. import util
from .._dependencies import Cluster

api = APIRouter()


class ArrayParams(BaseModel):
    plugin: Annotated[str, Field(min_length=1, description="Plugin name (entry point in simplyblock.array_plugins), e.g. aws-ebs")]
    endpoint: str = ""
    options: dict[str, Any] = Field(default_factory=dict)
    # Stored in the KMS only; masked in every representation of this model.
    credentials: dict[str, SecretStr] = Field(default_factory=dict)
    lun_size: Annotated[util.Size, Field(description="Size of the LUN created for each added node; 0 = none")] = 0


class ArrayDTO(BaseModel):
    id: str
    plugin: str
    endpoint: str
    options: dict[str, Any]
    lun_size: int
    has_credentials: bool
    capabilities: dict[str, Any] | None = None


@api.post('/', name='clusters:arrays:create', status_code=201, response_model=ArrayDTO)
def register(cluster: Cluster, parameters: ArrayParams) -> ArrayDTO:
    """Register the cluster's storage array (or re-register it, e.g. to
    rotate its credentials). The plugin is loaded and connected with the
    credentials and its capabilities read before anything is saved."""
    try:
        view = array_controller.register_array(
            cluster.get_id(), parameters.plugin, parameters.endpoint, parameters.options,
            parameters.credentials, parameters.lun_size)
    except ValueError as e:
        raise HTTPException(422, str(e))
    return ArrayDTO(**view)


@api.get('/', name='clusters:arrays:list', response_model=list[ArrayDTO])
def list_arrays(cluster: Cluster) -> list[ArrayDTO]:
    return [ArrayDTO(**a) for a in array_controller.get_arrays(cluster.get_id())]


@api.get('/{array_id}', name='clusters:arrays:get', response_model=ArrayDTO)
def get(cluster: Cluster, array_id: str) -> ArrayDTO:
    for a in array_controller.get_arrays(cluster.get_id()):
        if a["id"] == array_id:
            return ArrayDTO(**a)
    raise HTTPException(404, f"array {array_id} not found")


@api.delete('/{array_id}', name='clusters:arrays:delete', status_code=204,
            responses={204: {"content": None}})
def delete(cluster: Cluster, array_id: str, force: bool = False) -> Response:
    """Remove the registration and its credentials. Refused while LUNs
    created through the array exist, unless ``force``."""
    try:
        array_controller.delete_array(cluster.get_id(), array_id, force=force)
    except KeyError as e:
        raise HTTPException(404, str(e.args[0] if e.args else e))
    except ValueError as e:
        raise HTTPException(409, str(e))
    return Response(status_code=204)
