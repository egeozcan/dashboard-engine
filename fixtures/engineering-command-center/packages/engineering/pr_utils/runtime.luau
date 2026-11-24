--!strict
local Utils = {}

function Utils.extract_jira_key(title)
    if type(title) ~= "string" then
        return nil
    end
    return string.match(title, "([A-Z][A-Z0-9]+%-%d+)")
end

return Utils
