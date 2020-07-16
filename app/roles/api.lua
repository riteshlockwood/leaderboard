local cartridge = require('cartridge')
local vshard = require('vshard')
local errors = require('errors')
local log = require('log')
local err_vshard_router = errors.new_class("Vshard routing error")
local err_httpd = errors.new_class("httpd error")
local checks = require('checks')

--local function init(opts) -- luacheck: no unused args
--    -- if opts.is_master then
--    -- end
--
--    local httpd = cartridge.service_get('httpd')
--    httpd:route({method = 'GET', path = '/hello'}, function()
--        return {body = 'Hello world!'}
--    end)
--
--    return true
--end
--
--local function stop()
--end
--
--local function validate_config(conf_new, conf_old) -- luacheck: no unused args
--    return true
--end
--
--local function apply_config(conf, opts) -- luacheck: no unused args
--    -- if opts.is_master then
--    -- end
--
--    return true
--end

local function json_response(req, json, status)
    checks('table', 'table', 'uint64')

    local resp = req:render({json = json})
    resp.status = status
    return resp
end

local function internal_error_response(req, error)
    checks('table', 'table')
    local resp = json_response(req, {
        info = "Internal error",
        error = error
    }, 500)
    return resp
end

local function profile_conflict_response(req)
    checks('table')
    local resp = json_response(req, {
        info = "Leader already exist"
    }, 409)
    return resp
end

local function storage_error_response(req, error)
    checks('table', 'table')
    if error.err == "Leader already exist" then
        return profile_conflict_response(req)
    --elseif error.err == "Profile not found" then
    --    return profile_not_found_response(req)
    --elseif error.err == "Unauthorized" then
    --    return profile_unauthorized(req)
    else
        return internal_error_response(req, error)
    end
end

local function http_leaderboard_add_leader(req)
    checks('table')
    local leader = req:json()

    local router = cartridge.service_get('vshard-router').get()
    local bucket_id = router:bucket_id(leader.leader_id)
    leader.bucket_id = bucket_id
    log.info("before pcall")
    local resp, error = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'write',
        'leaderboard_add_leader',
        {leader}
    )
    log.info("after pcall")
    if error then
        log.info("error")
        return internal_error_response(req, error)
    end
    if resp.error then
        return storage_error_response(req, resp.error)
    end

    return json_response(req, {info = "Successfully Added the leader"}, 201)
end

local function http_leaderboard_get_rank(req)
    checks('table')
    local leader_id = tonumber(req:stash('leader_id'))
    --local password = req:json().password
    local router = cartridge.service_get('vshard-router').get()
    local bucket_id = router:bucket_id(leader_id)

    local resp, error = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'read',
        'leaderboard_get_rank',
        {leader_id}
    )

    if error then
        return internal_error_response(req, error)
    end
    if resp.error then
        return storage_error_response(req, resp.error)
    end

    return json_response(req, resp.leader, 200)
end

local function init(opts)
    checks('table')
    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write',
            'universe',
            nil, { if_not_exists = true }
        )
    end

    local httpd = cartridge.service_get('httpd')

    if not httpd then
        return nil, err_httpd:new("not found")
    end

    log.info("Starting httpd")

    httpd:route(
        { path = '/leader', method = 'POST', public = true },
        http_leaderboard_add_leader
    )
    httpd:route(
        { path = '/leader/:leader_id', method = 'GET', public = true },
        http_leaderboard_get_rank
    )
    --httpd:route(
    --    { path = '/leader/:leader_id', method = 'PUT', public = true },
    --    http_leaderboard_update_score
    --)

    --httpd:route(
    --    {path = '/leader/:leader_id', method = 'GET', public = true},
    --    http_leaderboard_get_adjacent_ranks
    --)
    --httpd:route(
    --    {path = '/leader/:leader_id', method = 'DELETE', public = true},
    --    http_leaderboard_delete_leader
    --)

    log.info("Created httpd")
    return true
end


return {
    role_name = 'app.roles.api',
    init = init,
    --stop = stop,
    --validate_config = validate_config,
    --apply_config = apply_config,
     dependencies = {'cartridge.roles.vshard-router'},
}
