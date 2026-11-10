

# RepositoryDeletion


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**repository** | **String** |  |  |
|**storageNamespace** | **String** |  |  |
|**state** | [**StateEnum**](#StateEnum) |  |  |
|**objectsDeleted** | **Long** |  |  |
|**requestedBy** | **String** |  |  |
|**requestedAt** | **Long** | Unix Epoch in seconds |  |
|**updatedAt** | **Long** | Unix Epoch in seconds |  |
|**attempts** | **Integer** |  |  |
|**lastError** | **String** |  |  [optional] |



## Enum: StateEnum

| Name | Value |
|---- | -----|
| PENDING | &quot;pending&quot; |
| DELETING | &quot;deleting&quot; |
| DONE | &quot;done&quot; |
| FAILED | &quot;failed&quot; |



