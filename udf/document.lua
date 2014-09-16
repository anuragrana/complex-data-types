

function set(record, docBin, element, value)
  if aerospike:exists(record) then
  end
end

function get(record, docBin, elements)
  debug('Element is: %s', tostring(elements))
  if aerospike:exists(record) then
    debug('DocBin: %s', tostring(record[docBin]))
    local current = record[docBin]
    if type(elements) == 'string' then
      current = current[elements]
      debug('current: %s', tostring(current))
    else
      for node in list.iterator(elements) do
          current = current[node]
      end
    end 
    debug('current: %s', tostring(current))
    return current
  else
    return nil
  end
end
