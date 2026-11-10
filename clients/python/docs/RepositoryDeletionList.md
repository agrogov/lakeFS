# RepositoryDeletionList


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**pagination** | [**Pagination**](Pagination.md) |  | 
**results** | [**List[RepositoryDeletion]**](RepositoryDeletion.md) |  | 

## Example

```python
from lakefs_sdk.models.repository_deletion_list import RepositoryDeletionList

# TODO update the JSON string below
json = "{}"
# create an instance of RepositoryDeletionList from a JSON string
repository_deletion_list_instance = RepositoryDeletionList.from_json(json)
# print the JSON string representation of the object
print RepositoryDeletionList.to_json()

# convert the object into a dict
repository_deletion_list_dict = repository_deletion_list_instance.to_dict()
# create an instance of RepositoryDeletionList from a dict
repository_deletion_list_form_dict = repository_deletion_list.from_dict(repository_deletion_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


