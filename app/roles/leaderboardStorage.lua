---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by riteshpatel.
--- DateTime: 14/07/2020 11:56
---
local checks = require('checks')
local errors = require('errors')
local leaderboard_err_storage = errors.new_class("Leaderboard Storage error")
local log = require('log')

local function init_space()
    local leader = box.schema.space.create(
        'leader',
        {
            format = {
                {'leader_id', 'unsigned'},
                {'bucket_id', 'unsigned'},
                {'score', 'unsigned'}
            },

            if_not_exists = true,
        }
    )


    leader:create_index('leader_id', {
        parts = {'leader_id'},
        if_not_exists = true,
    })

    leader:create_index('bucket_id', {
        parts = {'bucket_id'},
        unique = false,
        if_not_exists = true,
    })
end

local function leaderboard_add_leader(leader)
    checks('table')
    log.info("leaderboard_add_leader 1")
    local exist = box.space.leader:get(leader.leader_id)
    if exist ~= nil then
        log.info("Leader already exist")
        return {ok = false, error = leaderboard_err_storage:new("Leader already exist")}
    end

    box.space.leader:insert(box.space.leader:frommap(leader))
    log.info("leaderboard_add_leader 3")
    return {ok = true, error = nil}
end

local function init(opts)
    if opts.is_master then
        init_space()

        box.schema.func.create('leaderboard_add_leader', {if_not_exists = true})
        --box.schema.func.create('profile_get', {if_not_exists = true})
        --box.schema.func.create('profile_update', {if_not_exists = true})
        --box.schema.func.create('profile_delete', {if_not_exists = true})
    end

    rawset(_G, 'leaderboard_add_leader', leaderboard_add_leader)
    --rawset(_G, 'profile_get', profile_get)
    --rawset(_G, 'profile_update', profile_update)
    --rawset(_G, 'profile_delete', profile_delete)

    return true
end

return {
    role_name = 'leaderboard_storage',
    init = init,
    utils = {
        leaderboard_add_leader = leaderboard_add_leader,
        --profile_update = profile_update,
        --profile_get = profile_get,
        --profile_delete = profile_delete,
    },
    dependencies = {
        'cartridge.roles.vshard-storage'
    }
}
