--- The set function sets a value in a document
-- The document is an Aerospike List or Map stored in a Bin 
-- containing other Lists and/or Maps
-- @param record The record containing the document Bin
-- @param docBin The name of the document Bin
-- @param path A list representing a path to the element to be modified. This is an Aerospike List.
-- @param value The value to be 'set' at the location specified by path
-- @author Peter
function set(record, docBin, path, value)
  
  local first = record[docBin]
  local current = record[docBin]
  local container = nil
  local element = nil
  if aerospike:exists(record) then
    for node in list.iterator(path) do
      container = current
      current = current[node]
      element = node
    end
    --debug('Types c:%s l:%s v:%s', tostring(element), tostring(container), tostring(value))
    container[element] = value
    record[docBin] = first
    aerospike:update(record)
    return record[docBin]
  else
    return 'not found'
  end
end

--- The get function gets a value from a document
-- The document is an Aerospike List or Map stored in a Bin 
-- containing other Lists and/or Maps
-- @param record The record containing the document Bin
-- @param docBin The name of the document Bin
-- @param path A list representing a path to the element to be modified. This is an Aerospike List.
-- @author Peter
function get(record, docBin, path)
  --debug('Element is: %s', tostring(path))
  if aerospike:exists(record) then
    --debug('DocBin: %s', tostring(record[docBin]))
    local current = record[docBin]
    if type(path) == 'string' then
      current = current[path]
      --debug('current: %s', tostring(current))
    else
      for node in list.iterator(path) do
        current = current[node]
      end
    end 
    --debug('current: %s', tostring(current))
    return current
  else
    return nil
  end
end
