// Included in fencing.cpp. Both systems keep their own local OS lock and
// durable registry. Only the signed, target-bound handoff crosses systems.
namespace {
std::string decode_key(const std::string& hex) {
    if(hex.size()!=64 || hex.find_first_not_of("0123456789abcdef")!=std::string::npos)
        fail("HANDOFF_KEY","Expected 32 bytes encoded as 64 lowercase hexadecimal characters");
    auto digit=[](char ch){return ch<='9'?ch-'0':ch-'a'+10;};
    std::string bytes(32,'\0');
    for(std::size_t i=0;i<bytes.size();++i)bytes[i]=static_cast<char>(16*digit(hex[2*i])+digit(hex[2*i+1]));
    return bytes;
}
std::string handoff_mac(const std::string& key,const std::string& message) {
    auto bytes=decode_key(key);
    std::string inner(64,'\x36'), outer(64,'\x5c');
    for(std::size_t i=0;i<bytes.size();++i){inner[i]^=bytes[i];outer[i]^=bytes[i];}
    PortableSha256 a;a.add(inner.data(),inner.size());a.add(message.data(),message.size());
    auto raw=decode_key(a.finish());
    PortableSha256 b;b.add(outer.data(),outer.size());b.add(raw.data(),raw.size());return b.finish();
}
json sign_handoff(json payload,const std::string& key) {
    auto tag=handoff_mac(key,payload.dump());
    payload["mac"]=tag;
    return payload;
}
json check_handoff(const json& envelope,const char* kind,const std::string& uuid,const std::string& key) {
    if(!envelope.is_object()||!envelope.contains("mac")||!envelope.at("mac").is_string())
        fail("HANDOFF_PROOF","Missing handoff signature");
    auto value=envelope;auto tag=value.at("mac").get<std::string>();value.erase("mac");
    auto expected=handoff_mac(key,value.dump());
    unsigned diff=0;
    if(tag.size()==expected.size())for(std::size_t i=0;i<tag.size();++i)diff|=expected[i]^tag[i];
    if(tag.size()!=expected.size()||diff||value.value("format","")!=kind||value.value("uuid","")!=uuid)
        fail("HANDOFF_PROOF","Handoff signature, kind or database identity does not match");
    return value;
}
// Called exclusively under the UUID lease. An absent target domain is never
// recreated during accept: a copied data directory alone carries no authority.
std::string handoff_domain(const fs::path& root,const std::string& uuid,const std::string& key,bool create) {
    auto path=root/("control-handoff-domain-"+uuid+".json");
    if(!native_exists(path)){
        if(!create)fail("HANDOFF_OFFER_REQUIRED","Target must issue an offer from this local domain first");
        auto envelope=sign_handoff({{"format","EduDBHandoffDomain1"},{"uuid",uuid},{"domain",new_identity()}},key);
        persist(path,envelope);
    }
    ReadOnlyFile file(path,true);auto size=file.size();
    if(size<2||size>4096)fail("HANDOFF_PROOF","Invalid local domain record");
    std::string bytes(size,'\0');file.read(0,reinterpret_cast<unsigned char*>(bytes.data()),bytes.size());
    auto sealed=json::parse(bytes);auto checksum=sealed.at("checksum").get<std::uint32_t>();sealed.erase("checksum");
    if(crc(sealed.dump())!=checksum)fail("FENCE_CORRUPT","Damaged handoff domain");
    auto state=check_handoff(sealed,"EduDBHandoffDomain1",uuid,key);
    auto domain=state.at("domain").get<std::string>();identity_ok(domain);return domain;
}
}

json FenceDomain::offer(const WriterIdentity& standby,std::uint64_t replayed,const std::string& key) {
    identity_ok(standby.instance_id);identity_ok(standby.timeline);
    auto lease=impl_->lease(5000);
    auto domain=handoff_domain(impl_->root,impl_->uuid,key,true);
    return sign_handoff({{"format","EduDBHandoffOffer1"},{"uuid",impl_->uuid},{"target_domain",domain},
                         {"target",standby.instance_id},{"parent_timeline",standby.timeline},{"fork_lsn",replayed}},key);
}

json FenceDomain::surrender(const WriterIdentity& owner,std::uint64_t durable,const json& envelope,const std::string& key) {
    auto offer=check_handoff(envelope,"EduDBHandoffOffer1",impl_->uuid,key);
    auto target=offer.at("target").get<std::string>();identity_ok(target);
    auto domain=offer.at("target_domain").get<std::string>();identity_ok(domain);
    if(target==owner.instance_id || offer.at("parent_timeline")!=owner.timeline || offer.at("fork_lsn")!=durable)
        fail("HANDOFF_NOT_CAUGHT_UP","Candidate instance, source timeline or last durable LSN is unsuitable");
    auto lock=impl_->lease(5000);
    auto local=handoff_domain(impl_->root,impl_->uuid,key,true);
    if(local==domain)fail("HANDOFF_DOMAIN","Use local promotion within a single fencing domain");
    auto state=load_state(impl_->state_path,impl_->uuid);
    if(state.contains("departed")) {
        if(state.at("departed").at("offer")==envelope && state.at("owner_instance_id")==owner.instance_id
           && state.at("timeline")==owner.timeline && state.at("epoch")==owner.epoch)
            return state.at("departed").at("ticket");
        fail("FENCED","Ownership has been surrendered to a different target");
    }
    if(!impl_->matches(state,owner))fail("FENCED","Only the current primary may release its ownership");
    if(owner.epoch==std::numeric_limits<std::uint64_t>::max())fail("FENCE_LIMIT","Fence epoch exhausted");
    auto ticket=sign_handoff({{"format","EduDBHandoffTicket1"},{"uuid",impl_->uuid},{"source",owner.instance_id},
        {"source_domain",local},{"target",target},{"target_domain",domain},{"parent_timeline",owner.timeline},
        {"timeline",new_identity()},{"fork_lsn",durable},{"epoch",owner.epoch+1}},key);
    state["departed"]={{"offer",envelope},{"ticket",ticket}};
    persist(impl_->state_path,state); // Force old-writer rejection before exposing the ticket.
    return ticket;
}

FencePermit FenceDomain::accept(const WriterIdentity& standby,std::uint64_t replayed,const json& envelope,const std::string& key,bool already_primary) {
    auto ticket=check_handoff(envelope,"EduDBHandoffTicket1",impl_->uuid,key);
    auto permit=std::make_unique<FencePermit::Impl>();permit->lease=impl_->lease(5000);
    auto local=handoff_domain(impl_->root,impl_->uuid,key,false);
    if(ticket.at("target_domain")!=local || ticket.at("target")!=standby.instance_id)
        fail("HANDOFF_TARGET","Ticket is scoped to a different participant");
    if(already_primary){
        auto state=load_state(impl_->state_path,impl_->uuid);
        if(state.contains("departed"))
            fail("FENCED","This instance has already surrendered the accepted timeline");
        if(!impl_->matches(state,standby)||state.value("accepted_ticket",json())!=envelope)
            fail("HANDOFF_TARGET","Primary has no matching completed transfer");
        permit->state=state;return FencePermit(std::move(permit));
    }
    if(ticket.at("fork_lsn")!=replayed || ticket.at("parent_timeline")!=standby.timeline)
        fail("HANDOFF_TARGET","Ticket does not match this standby, replay position and local authority");
    auto epoch=ticket.at("epoch").get<std::uint64_t>();
    if(epoch<2)fail("HANDOFF_PROOF","Invalid successor epoch");
    auto timeline=ticket.at("timeline").get<std::string>();identity_ok(timeline);
    if(native_exists(impl_->state_path)) {
        auto old=load_state(impl_->state_path,impl_->uuid);
        if(!old.contains("departed") && old.value("accepted_ticket",json())==envelope &&
           old.at("epoch")==epoch && old.at("timeline")==timeline && old.at("owner_instance_id")==standby.instance_id) {
            permit->state=old;return FencePermit(std::move(permit));
        }
        if(!old.contains("departed") || old.at("departed").at("ticket").at("epoch").get<std::uint64_t>()>=epoch)
            fail("FENCE_STALE","This domain has an active or newer ownership record");
    }
    permit->state={{"format","EduDBFence1"},{"uuid",impl_->uuid},{"epoch",epoch},{"owner_instance_id",standby.instance_id},
        {"timeline",timeline},{"parent_timeline",standby.timeline},{"fork_lsn",replayed},{"accepted_ticket",envelope}};
    persist(impl_->state_path,permit->state);
    return FencePermit(std::move(permit));
}
