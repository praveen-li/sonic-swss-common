redis.replicate_commands()
local ret = {}
local tablename = KEYS[2]
local stateprefix = ARGV[2]
-- skip the batch size for now, pop everything in keyset
-- in case notification is redudent (keyset empty), skip
local keyset_num = redis.call('SCARD', KEYS[1])
if( keyset_num == 0 ) then
    return ret
end
local keys = redis.call('SPOP', KEYS[1], tostring(keyset_num))
local n = table.getn(keys)
for i = 1, n do
   local key = keys[i]
   -- Check if there was request to delete the key, clear it in table first
   local num = redis.call('SREM', KEYS[3], key)
   if num == 1 then
      redis.call('DEL', tablename..key)
   end
   -- Push the new set of field/value for this key in table
   local fieldvalues = redis.call('HGETALL', stateprefix..tablename..key)
   table.insert(ret, {key, fieldvalues})
   for i = 1, #fieldvalues, 2 do
      redis.call('HSET', tablename..key, fieldvalues[i], fieldvalues[i + 1])
   end
   -- Clean up the key in temporary state table
   redis.call('DEL', stateprefix..tablename..key)
end
return ret
