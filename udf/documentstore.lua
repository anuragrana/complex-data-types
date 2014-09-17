--
-- Document store API
--
-- User can pass in Document as a string... 
--
-- Requires: http://regex.info/code/JSON.lua
--
-- set
-- get
local function jsonstr_to_ltab(json_str) 
	JSON = require("JSON")
	return JSON:decode(json_str)
end

local function ltab_to_jsonstr(ltab_value)
	JSON = require("JSON")
	return JSON:encode(ltab_value)
end


local function UPDATE(rec)
    if aerospike:exists(rec) then
        aerospike:update(rec)
    else
        aerospike:create(rec)
    end
end

function set(rec, bin, json_str) 
	if (type(json_str) ~= 'string') then
		return "Type Not String"
	end
	local json_ltab = jsonstr_to_ltab(json_str);
	rec[bin] = json_str
	UPDATE(rec)
	return json_str
end

local function setval(ltab, arg, level, val)
	if (level == (#arg - 1)) then
		ltab[arg[level]] = val
		return val
	else
		return setval(ltab[arg[level]], arg, level+1)
	end
end

function update(rec, bin, ...)
	json_str = rec[bin]
	if (type(json_str) ~= "string") then
		return "Type Not String"
	end
	ltab = jsonstr_to_ltab(json_str)
	setval(ltab, arg, 1)
	rec[bin] = ltab_to_jsonstr(ltab)
	UPDATE(rec)
	return rec
end

local function getval(ltab, arg, level)
	if (level == #arg) then
		return ltab[arg[level]]
	else
		return getval(ltab[arg[level]], arg, level+1)
	end
end

function get(rec, bin, ...)
	json_str = rec[bin]
	if (type(json_str) ~= "string") then
		return "Type Not String"
	end
	ltab = jsonstr_to_ltab(json_str)
	return ltab_to_jsonstr(getval(ltab, arg, 1))
end
