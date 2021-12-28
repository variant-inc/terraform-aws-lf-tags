<#
description:
desired_list = Read JSON and make a list of unique LF-TAG keys and their possible values - read file
real_list = List LF-TAGs keys and possible values - aws lakeformation list-lf-tags

compare if two lists contain same tags and their values - desired_list == real_list
if they don't, update by using info from desired_list
    create tag and/or it's possible values - aws lakeformation create-lf-tag
    remove unused tags - aws lakeformation delete-lf-tag
    and/or values - aws lakeformation update-lf-tag

resource_tags = get tags from each Data Lake resource (db, table, column) and save them to a list of objects matching input JSON structure
databases - aws glue get-databases + aws lakeformation get-resource-lf-tags
tables/columns - aws glue get-tables + aws lakeformation get-resource-lf-tags
    itterate through all objects and their tags/values
        un/assign correct tags/values to Data Lake resources as specified in JSON - aws lakeformation add-lf-tags-to-resource/remove-lf-tags-from-resource
#>

[CmdletBinding()]
param (
    # Input json with resulting tags on Data Lake resources
    [Parameter()]
    [PSCustomObject]
    [ValidateNotNullOrEmpty()]
    $InputJson
)

function Get-CleanDesiredLFTags {
    $CleanDesiredLFTags = @()
    foreach($DB in ($InputJson | ConvertFrom-Json).databases) {
        foreach($tag in $DB.tags | get-member -MemberType NoteProperty | Select-Object -ExpandProperty Name){
            if ($tag -notin $($CleanDesiredLFTags.TagKey)) {
                $CleanDesiredLFTags += [PSCustomObject]@{
                    "TagKey" = $tag
                    "TagValues" = @($DB.tags.$tag)
                }
            } else {
                $CleanDesiredLFTags | ForEach-Object {
                    if ($_.tagkey -eq $tag){
                        $_.tagvalues += $DB.tags.$tag
                    }
                }
            }
        }
    }

    foreach ($table in ($InputJson | ConvertFrom-Json).databases.tables) {
        foreach ($tag in $($table.tags | get-member -MemberType NoteProperty | Select-Object -ExpandProperty Name)) {
            if ($tag -notin $($CleanDesiredLFTags.TagKey)) {
                $CleanDesiredLFTags += [PSCustomObject]@{
                    "TagKey" = $tag
                    "TagValues" = @($table.tags.$tag)
                }
            } else {
                $CleanDesiredLFTags | ForEach-Object {
                    if ($_.tagkey -eq $tag){
                        $_.tagvalues += $table.tags.$tag
                    }
                }
            }
        }
    }

    foreach ($column in $(($InputJson | ConvertFrom-Json).databases.tables.columns | Where-Object { $_ })) {
        foreach ($tag in $($column.tags | get-member -MemberType NoteProperty | Select-Object -ExpandProperty Name)) {
            if ($tag -notin $($CleanDesiredLFTags.TagKey)) {
                $CleanDesiredLFTags += [PSCustomObject]@{
                    "TagKey" = $tag
                    "TagValues" = @($column.tags.$tag)
                }
            } else {
                $CleanDesiredLFTags | ForEach-Object {
                    if ($_.tagkey -eq $tag){
                        $_.tagvalues += $column.tags.$tag
                    }
                }
            }
        }
    }

    $CleanDesiredLFTags | ForEach-Object {
        $_.TagValues = $_.TagValues | Select-Object -Unique
        if ($_.TagValues.GetType().Name -eq "String"){
            $_.TagValues = @($_.TagValues)
        }
    }

    return $CleanDesiredLFTags
}

#region Update LF-tags state
# Get existing LF-tags from AWS and make clean list
$CurrentLFTags = aws lakeformation list-lf-tags
$CleanCurrentLFTags = ($CurrentLFTags | convertfrom-json).LFTags | Select-Object TagKey, TagValues

# Make a clean list of tags from JSON
$CleanDesiredLFTags = Get-CleanDesiredLFTags

# Compare 2 lists
$ComparisonResult = Compare-Object $CleanCurrentLFTags.TagKey $CleanDesiredLFTags.TagKey
if ($ComparisonResult.length -eq 0){
    Write-Output "All LF-tag keys are existing as defined in JSON."
} else {
    foreach ($Result in $ComparisonResult) {
        if ($Result.SideIndicator -eq "=>") {
            # Create LF-tag and values on AWS
            aws lakeformation create-lf-tag --tag-key $Result.InputObject --tag-values ($CleanDesiredLFTags | Where-Object TagKey -eq $Result.InputObject).TagValues | Out-Null
            Write-Output "CREATE: $($Result.InputObject) with $(($CleanDesiredLFTags | Where-Object TagKey -eq $Result.InputObject).TagValues)"
        } else {
            # Remove LF-tag and values on AWS
            aws lakeformation delete-lf-tag --tag-key $Result.InputObject | Out-Null
            Write-Output "DELETE: $($Result.InputObject)"
        }
    }
}

foreach ($Tag in $CleanCurrentLFTags.TagKey) {
    $ValuesToAdd = @()
    $ValuesToDelete = @()
    try {
        $CompareValues = Compare-Object ($CleanCurrentLFTags | Where-Object TagKey -eq $Tag).TagValues ($CleanDesiredLFTags | Where-Object TagKey -eq $Tag).TagValues
    }
    catch {
        $CompareValues = @()
    }
    if ($CompareValues.length -eq 0) {
        Continue
    } else {
        foreach ($ComparedValue in $CompareValues) {
            if ($ComparedValue.SideIndicator -eq "=>") {
                # Add values on AWS
                $ValuesToAdd += $ComparedValue.InputObject
            } else {
                # Remove values on AWS
                $ValuesToDelete += $ComparedValue.InputObject
            }
        }
        Write-Output "$Tag values to add: $ValuesToAdd"
        Write-Output "$Tag values to delete: $ValuesToDelete"
        if ($ValuesToAdd.Length -gt 0 -and $ValuesToDelete.Length -gt 0) {
            aws lakeformation update-lf-tag --tag-key $Tag --tag-values-to-delete $ValuesToDelete --tag-values-to-add $ValuesToAdd | Out-Null
        } elseif ($ValuesToAdd.Length -gt 0 -and $ValuesToDelete.Length -eq 0) {
            aws lakeformation update-lf-tag --tag-key $Tag --tag-values-to-add $ValuesToAdd | Out-Null
        } elseif ($ValuesToAdd.Length -eq 0 -and $ValuesToDelete.Length -gt 0) {
            aws lakeformation update-lf-tag --tag-key $Tag --tag-values-to-delete $ValuesToDelete | Out-Null
        }
    }
}
#endregion

#region Update LF-tags assignements
# Databases
$DBs = (aws glue get-databases | convertfrom-json).DatabaseList.Name
foreach($DB in ($InputJson | ConvertFrom-Json).databases) {
    $ResourceQuery = ""
    if ($DB.dbname -in $DBs) {
        $ResourceQuery = $(-join('{\"Database\": {\"Name\": \"', $DB.dbname, '\"}}'))
        $CurrentDBTags = (aws lakeformation get-resource-lf-tags --resource $ResourceQuery | ConvertFrom-Json).LFTagOnDatabase | Select-Object TagKey, TagValues
        $DesiredDBTags = @((($InputJson | ConvertFrom-Json).databases | Where-Object dbname -eq $DB.dbname).tags.psobject.properties | Where-Object MemberType -eq "NoteProperty" | Select-Object @{Name="TagKey"; Expression={$_.Name}}, @{Name="TagValues"; Expression={$_.Value}})
        $DesiredDBTags | ForEach-Object {
            if ($_.TagValues.GetType().Name -eq "String"){
                $_.TagValues = @($_.TagValues)
            }
        }
        $ComparisonResult = Compare-Object $CurrentDBTags.TagKey $DesiredDBTags.TagKey -IncludeEqual
        $TagsToAdd = @()
        $TagsToDelete = @()
        foreach ($Result in $ComparisonResult) {
            if ($Result.SideIndicator -eq "<=") {
                # Remove LF-tag and values from DBs
                $TagsToDelete += $Result.InputObject
            } else {
                # Add or Update LF-tag and value
                $TagsToAdd += $Result.InputObject
            }
        }
        if ($TagsToAdd.length -gt 0) {
            $TagsQuery = "["
            foreach ($Tag in $TagsToAdd) {
                $TagsQuery += $(-join("{\`"TagKey\`":\`"", $Tag, "\`",\`"TagValues\`":[\`"", $(($DesiredDBTags | Where-Object TagKey -eq $Tag).TagValues), "\`"]},"))
            }
            $TagsQuery = $TagsQuery -replace ".$"
            $TagsQuery += "]"
            aws lakeformation add-lf-tags-to-resource --resource $ResourceQuery --lf-tags $TagsQuery | Out-Null
        }
        if ($TagsToDelete.length -gt 0) {
            $TagsQuery = "["
            foreach ($Tag in $TagsToDelete) {
                $TagsQuery += $(-join('{\"TagKey\":\"', $Tag, '\",\"TagValues\":[\"', $(($CurrentDBTags | Where-Object TagKey -eq $Tag).TagValues), '\"]},'))
            }
            $TagsQuery = $TagsQuery -replace ".$"
            $TagsQuery += "]"
            aws lakeformation remove-lf-tags-from-resource --resource $ResourceQuery --lf-tags $TagsQuery | Out-Null
        }
        
        # Tables
        $Tables = (aws glue get-tables --database $DB.dbname | ConvertFrom-Json).TableList
        foreach ($Table in $DB.tables) {
            if ($Table.tablename -in $Tables.Name) {
                $ResourceQuery = $(-join('{\"Table\": {\"DatabaseName\": \"', $DB.dbname, '\", \"Name\": \"', $Table.tablename, '\"}}'))
                $CurrentTableColumnTags = aws lakeformation get-resource-lf-tags --resource $ResourceQuery | ConvertFrom-Json
                $DesiredTableTags = @(((($InputJson | ConvertFrom-Json).databases | Where-Object dbname -eq $DB.dbname).tables |Where-Object tablename -eq $Table.tablename).tags.psobject.properties | Where-Object MemberType -eq "NoteProperty" | Select-Object @{Name="TagKey"; Expression={$_.Name}}, @{Name="TagValues"; Expression={$_.Value}})
                $DesiredTableTags | ForEach-Object {
                    if ($_.TagValues.GetType().Name -eq "String"){
                        $_.TagValues = @($_.TagValues)
                    }
                }
    
                # This part allows for overriding DB tag inheritance on table level
                $DesiredTableDBTags = $DesiredTableTags + $DesiredDBTags
                $FinalDesiredTableTags = @()
            
                $DesiredTableDBTags | ForEach-Object {
                    if ($_.TagKey -notin $FinalDesiredTableTags.TagKey){
                        $FinalDesiredTableTags += $_
                    }
                }
    
                $ComparisonResult = Compare-Object $CurrentTableColumnTags.LFTagsOnTable.TagKey $FinalDesiredTableTags.TagKey -IncludeEqual
                $TagsToAdd = @()
                $TagsToDelete = @()
                foreach ($Result in $ComparisonResult) {
                    if ($Result.SideIndicator -eq "<=") {
                        # Remove LF-tag and values from Tables
                        $TagsToDelete += $Result.InputObject
                    } else {
                        # Add or Update LF-tag and value
                        $TagsToAdd += $Result.InputObject
                    }
                }
                if ($TagsToAdd.length -gt 0) {
                    $TagsQuery = "["
                    foreach ($Tag in $TagsToAdd) {
                        $TagsQuery += $(-join("{\`"TagKey\`":\`"", $Tag, "\`",\`"TagValues\`":[\`"", $(($FinalDesiredTableTags | Where-Object TagKey -eq $Tag).TagValues), "\`"]},"))
                    }
                    $TagsQuery = $TagsQuery -replace ".$"
                    $TagsQuery += "]"
                    aws lakeformation add-lf-tags-to-resource --resource $ResourceQuery --lf-tags $TagsQuery | Out-Null
                }
                if ($TagsToDelete.length -gt 0) {
                    $TagsQuery = "["
                    foreach ($Tag in $TagsToDelete) {
                        $TagsQuery += $(-join('{\"TagKey\":\"', $Tag, '\",\"TagValues\":[\"', $(($CurrentTableColumnTags.LFTagsOnTable | Where-Object TagKey -eq $Tag).TagValues), '\"]},'))
                    }
                    $TagsQuery = $TagsQuery -replace ".$"
                    $TagsQuery += "]"
                    aws lakeformation remove-lf-tags-from-resource --resource $ResourceQuery --lf-tags $TagsQuery | Out-Null
                }

                # Columns
                $Columns = $CurrentTableColumnTags.LFTagsOnColumns
                foreach ($Column in $($DB.tables | Where-Object tablename -eq $Table.tablename).columns) {
                    if ($Column.columnname -in $Columns.Name) {
                        $ResourceQuery = $(-join('{\"TableWithColumns\": {\"DatabaseName\": \"', $DB.dbname, '\", \"Name\": \"', $Table.tablename, '\", \"ColumnNames\": [\"', $Column.columnname, '\"]}}'))
                        $CurrentTableColumnTags = aws lakeformation get-resource-lf-tags --resource $ResourceQuery | ConvertFrom-Json
                        $DesiredColumnTags = @((((($InputJson | ConvertFrom-Json).databases | Where-Object dbname -eq $DB.dbname).tables | Where-Object tablename -eq $Table.tablename).columns | Where-Object columnname -eq $Column.columnname).tags.psobject.properties | Where-Object MemberType -eq "NoteProperty" | Select-Object @{Name="TagKey"; Expression={$_.Name}}, @{Name="TagValues"; Expression={$_.Value}})
                        $DesiredColumnTags | ForEach-Object {
                            if ($_.TagValues.GetType().Name -eq "String"){
                                $_.TagValues = @($_.TagValues)
                            }
                        }

                        # This part allows for overriding DB tag inheritance on table level
                        $DesiredColumnTableDBTags = $DesiredColumnTags + $DesiredTableTags + $DesiredDBTags
                        $FinalDesiredColumnTableTags = @()
                    
                        $DesiredColumnTableDBTags | ForEach-Object {
                            if ($_.TagKey -notin $FinalDesiredColumnTableTags.TagKey){
                                $FinalDesiredColumnTableTags += $_
                            }
                        }

                        $ComparisonResult = Compare-Object $CurrentTableColumnTags.LFTagsOnColumns.LFtags.TagKey $FinalDesiredColumnTableTags.TagKey -IncludeEqual
                        $TagsToAdd = @()
                        $TagsToDelete = @()
                        foreach ($Result in $ComparisonResult) {
                            if ($Result.SideIndicator -eq "<=") {
                                # Remove LF-tag and values from Tables
                                $TagsToDelete += $Result.InputObject
                            } else {
                                # Add or Update LF-tag and value
                                $TagsToAdd += $Result.InputObject
                            }
                        }
                        if ($TagsToAdd.length -gt 0) {
                            $TagsQuery = "["
                            foreach ($Tag in $TagsToAdd) {
                                $TagsQuery += $(-join("{\`"TagKey\`":\`"", $Tag, "\`",\`"TagValues\`":[\`"", $(($FinalDesiredColumnTableTags | Where-Object TagKey -eq $Tag).TagValues), "\`"]},"))
                            }
                            $TagsQuery = $TagsQuery -replace ".$"
                            $TagsQuery += "]"
                            aws lakeformation add-lf-tags-to-resource --resource $ResourceQuery --lf-tags $TagsQuery | Out-Null
                        }
                        if ($TagsToDelete.length -gt 0) {
                            $TagsQuery = "["
                            foreach ($Tag in $TagsToDelete) {
                                $TagsQuery += $(-join('{\"TagKey\":\"', $Tag, '\",\"TagValues\":[\"', $(($CurrentTableColumnTags.LFTagsOnTable | Where-Object TagKey -eq $Tag).TagValues), '\"]},'))
                            }
                            $TagsQuery = $TagsQuery -replace ".$"
                            $TagsQuery += "]"
                            aws lakeformation remove-lf-tags-from-resource --resource $ResourceQuery --lf-tags $TagsQuery | Out-Null
                        }
                    } else {
                        Write-Warning "Column: $($Column.columnname) Table: $($Table.tablename) DB: $($DB.dbname) does not exist in Data Lake."        
                    }
                }
            } else {
                Write-Warning "Table: $($Table.tablename) DB: $($DB.dbname) does not exist in Data Lake."
            }
        }
    } else {
        Write-Warning "DB: $($DB.dbname) does not exist on Data Lake."
    }
}
#endregion