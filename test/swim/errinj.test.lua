test_run = require('test_run').new()
errinj = box.error.injection

--
-- A complex case the payload cache needs to deal with. It is
-- possible, that a new member's incarnation is learned, but new
-- payload is not. That happens when a cluster is huge, and
-- anti-entropy with dissemination both may do not contain a
-- message sender. Payload cache should correctly process that,
-- when the new payload finally arrives.
--
s1 = swim.new({uuid = uuid(1), uri = uri(), heartbeat_rate = 1000, generation = 0})
s2 = swim.new({uuid = uuid(2), uri = uri(), heartbeat_rate = 1000, generation = 0})
s1_self = s1:self()
_ = s1:add_member({uuid = s2:self():uuid(), uri = s2:self():uri()})
_ = s2:add_member({uuid = s1_self:uuid(), uri = s1_self:uri()})
s1:size()
s2:size()
s1_view = s2:member_by_uuid(s1_self:uuid())


s1:set_payload('payload')
s1:self():incarnation()
-- Via probe() S2 learns new incarnation of S1, but without new
-- payload.
errinj.set("ERRINJ_SWIM_FD_ONLY", true)
s1:probe_member(s2:self():uri())
errinj.set("ERRINJ_SWIM_FD_ONLY", false)
s1_view:payload()
s1_view:incarnation()

s1:cfg({heartbeat_rate = 0.01})
s2:cfg({heartbeat_rate = 0.01})
while s1_view:payload() ~= 'payload' do fiber.sleep(0.01) end
p = s1_view:payload()
s1_view:payload() == p
p
s1_view:incarnation()

s1:delete()
s2:delete()
