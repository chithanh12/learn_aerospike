local function map_record(rec)
  local ret = map()
  for i, bin_name in ipairs(record.bin_names(rec)) do
    ret[bin_name] = rec[bin_name]
  end

  return ret
end

local function my_age_filter(min_age)
    return function(rec)
        if rec['age'] > min_age then
            return true
        end

        return false
    end
end

function age_filter(stream, min_age)
    local af = my_age_filter(min_age)
    return stream : filter(af) : map(map_record)
end