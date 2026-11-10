# RepositoryDeletion


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**repository** | **str** |  | 
**storage_namespace** | **str** |  | 
**state** | **str** |  | 
**objects_deleted** | **int** |  | 
**requested_by** | **str** |  | 
**requested_at** | **int** | Unix Epoch in seconds | 
**updated_at** | **int** | Unix Epoch in seconds | 
**attempts** | **int** |  | 
**last_error** | **str** |  | [optional] 

## Example

```python
from lakefs_sdk.models.repository_deletion import RepositoryDeletion

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryDeletion from a JSON string
repository_deletion_instance = RepositoryDeletion.from_json(json)
# print the JSON string representation of the object
print RepositoryDeletion.to_json()

# convert the object into a dict
repository_deletion_dict = repository_deletion_instance.to_dict()
# create an instance of RepositoryDeletion from a dict
repository_deletion_form_dict = repository_deletion.from_dict(repository_deletion_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


