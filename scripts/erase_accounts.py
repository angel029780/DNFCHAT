"""Process explicitly confirmed DNFCHAT deletions. Scheduled invocations are serialized."""
import json
import os
from datetime import datetime, timezone, timedelta
from uuid import uuid4
import firebase_admin
from firebase_admin import auth, credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

PROJECT = "dnfchat-2b3db"
def filtered(coll, key, op, value):
    return coll.where(filter=FieldFilter(key, op, value))

def match_link(key, uid):
    return key.startswith(uid + "_") or key.endswith("_" + uid)

def clean_comment(data, uid):
    if data.get("uid") != uid:
        return data
    return {"body": data["body"], "createdAt": data["createdAt"], "anonymous": True,"name":"Usuario eliminado"}

def clean_reaction(data, own):
    return {"emoji": data["emoji"], "anonymous": True,"name":"Usuario eliminado"} if own else data

def move(source, target, value):
    batch = db.batch()
    batch.set(target, value)
    batch.delete(source)
    batch.commit()

def audience(uid):
    blocked = (db.document(f"Users/{uid}/Private/preferences").get().to_dict() or {}).get("blocked", [])
    readers = set()
    for link in filtered(db.collection("FriendLinks"), "participants", "array_contains", uid).stream():
        d = link.to_dict()
        if d.get("status") != "accepted":
            continue
        for other in d["participants"]:
            if other == uid or other in blocked:
                continue
            pref = db.document(f"Users/{other}/Private/preferences").get().to_dict() or {}
            if uid not in pref.get("blocked", []):
                readers.add(other)
    return readers

def delete_tree(ref):
    for child in ref.collections():
        for doc in child.list_documents():
            delete_tree(doc)
    ref.delete()

def archive_posts(uid):
    readers = audience(uid)
    for post in db.collection(f"Feeds/{uid}/Posts").list_documents():
        source = post.get().to_dict()
        if source is None:
            delete_tree(post)
            continue
        archive_id = source.get("eraseArchiveId") or uuid4().hex
        archived = db.document("RemovedPosts/" + archive_id)
        # Persist the random destination before the first move so retries resume the same thread.
        if "eraseArchiveId" not in source:
            post.update({"eraseArchiveId": archive_id})
        archived.set({"createdAt": source["createdAt"]})
        for comment in post.collection("Comments").stream():
            move(comment.reference, archived.collection("Comments").document(comment.id),
                 clean_comment(comment.to_dict(), uid))
        for reaction in post.collection("Reactions").stream():
            own = reaction.id == uid
            destination = archived.collection("Reactions").document(uuid4().hex if own else reaction.id)
            move(reaction.reference, destination, clean_reaction(reaction.to_dict(), own))
        for reader in readers:
            db.document(f"ArchiveReaders/{reader}/ArchiveItems/{archive_id}").set({"createdAt": source["createdAt"]})
        delete_tree(post)
    delete_tree(db.document("Feeds/" + uid))

def preserve_shared_event(ref, uid):
    # Only the ownership field changes. Other members' photos and RSVPs remain.
    @firestore.transactional
    def transfer(tx):
        event = ref.get(transaction=tx).to_dict() or {}
        if event.get("owner") != uid:
            return
        group = ref.parent.parent.get(transaction=tx).to_dict() or {}
        successor = group.get("adminUid")
        if successor and successor != uid and successor in group.get("members", []):
            tx.update(ref, {"owner": successor})
        elif not group.get("members"):
            # Inactive group; the existing group erasure service will remove descendants.
            tx.delete(ref)
        else:
            raise RuntimeError("Resolve active event group ownership before erasure")
    transfer(db.transaction())

def erase_shared_activity(uid):
    for item in db.collection_group("FamilyPlan").select(["owner"]).stream():
        if item.id == "feedback" and item.to_dict().get("owner") == uid and item.reference.path.startswith("Chats/"):
            item.reference.delete()
    for item in db.collection_group("FamilyEvents").select(["owner"]).stream():
        if item.to_dict().get("owner") == uid and item.reference.path.startswith("Chats/"):
            preserve_shared_event(item.reference, uid)
    for item in db.collection_group("EventAttendance").select([]).stream():
        if item.id == uid and item.reference.path.startswith("Chats/"):
            item.reference.delete()
    for item in db.collection_group("FamilyEventPhotos").select(["owner"]).stream():
        if item.to_dict().get("owner") == uid and item.reference.path.startswith("Chats/"):
            item.reference.delete()
    for item in db.collection_group("MarketProducts").select(["owner"]).stream():
        if item.to_dict().get("owner") == uid and (item.reference.path.startswith("Chats/") or item.reference.parent.parent is None):
            if item.reference.parent.parent is None:
                batch = db.batch()
                batch.delete(db.collection("PublicMarketListings").document(item.id))
                batch.delete(item.reference)
                batch.commit()
            else:
                item.reference.delete()
    # Project only needed fields. Empty/missing parent documents also have collection-group descendants.
    for item in db.collection_group("Comments").stream():
        if item.to_dict().get("uid")==uid and item.reference.path.split("/")[0] in ("Feeds", "RemovedPosts"):
            item.reference.set(clean_comment(item.to_dict(), uid))
    for item in db.collection_group("Reactions").select(["emoji"]).stream():
        if item.id == uid and item.reference.path.split("/")[0] in ("Feeds", "RemovedPosts", "Chats"):
            move(item.reference, item.reference.parent.document(uuid4().hex), clean_reaction(item.to_dict(), True))
    for collection, root in [("Views","Feeds"), ("Requests","Chats"), ("Profiles","Chats"), ("Reporters","FeedReports")]:
        for item in db.collection_group(collection).select([]).stream():
            if item.id == uid and item.reference.path.split("/")[0] == root:
                item.reference.delete()
    for item in db.collection_group("Messages").select(["senderId","createdAt"]).stream():
        if item.to_dict().get("senderId")==uid and item.reference.path.startswith("Chats/"):
            item.reference.set({"type":"deleted", "senderId":"", "createdAt":item.get("createdAt")})
    for pref in db.collection_group("Private").select(["blocked"]).stream():
        if uid in pref.to_dict().get("blocked",[]) and pref.reference.path.startswith("Users/"):
            pref.reference.update({"blocked":firestore.ArrayRemove([uid])})
    for coll in ("Reports","Posts"):
        for item in db.collection_group(coll).select(["reporter"]).stream():
            if item.to_dict().get("reporter")==uid and item.reference.path.split("/")[0] == ("Chats" if coll=="Reports" else "FeedReports"):
                item.reference.delete()

def transfer_administered_groups(uid):
    # Members are appended when their join request is approved.
    for group in filtered(db.collection("Chats"), "members", "array_contains", uid).stream():
        if group.to_dict().get("adminUid") != uid:
            continue

        @firestore.transactional
        def transfer(tx):
            data = group.reference.get(transaction=tx).to_dict() or {}
            if data.get("adminUid") != uid:
                return
            config = db.document("AppConfig/moderation").get(transaction=tx).to_dict() or {}
            for candidate in data.get("members", []):
                if candidate == uid:
                    continue
                if db.document("AccountErasures/" + candidate).get(transaction=tx).exists:
                    continue
                try:
                    account = auth.get_user(candidate)
                except auth.UserNotFoundError:
                    continue
                if account.disabled or not account.email_verified or not account.email:
                    continue
                access = db.document("FamilyAccess/" + account.email.lower()).get(transaction=tx).to_dict() or {}
                if candidate != config.get("adminUid") and (access.get("status") != "active" or access.get("uid") != candidate):
                    continue
                tx.update(group.reference, {"adminUid": candidate})
                return
            raise RuntimeError("No eligible successor; an administrator must resolve group ownership")

        transfer(db.transaction())


def erase(uid, email, requested_at):
    archive_posts(uid)
    erase_shared_activity(uid)
    for group in filtered(db.collection("Chats"), "members", "array_contains", uid).stream():
        if group.to_dict().get("adminUid")==uid:
            raise RuntimeError("Transfer the group administrator in the Firebase console and retry")
        group.reference.update({"members":firestore.ArrayRemove([uid])})
    for link in db.collection("FriendLinks").list_documents():
        info=link.get().to_dict()
        if (info and uid in info.get("participants", [])) or (info is None and match_link(link.id, uid)):
            delete_tree(link)
    for item in db.collection("DirectReports").select(["linkId","reporter"]).stream():
        d=item.to_dict()
        if d.get("reporter")==uid or match_link(d.get("linkId",""),uid):
            item.reference.delete()
    for coll, key in [("StoryReports","owner"),("StoryReports","reporter"),("Invites","adminUid"),
                      ("UsernameReservations","uid"),("FriendDirectory","uid"),("FamilyAccess","uid")]:
        for item in filtered(db.collection(coll), key, "==", uid).select([]).stream():
            item.reference.delete()
    for root in ["FeedReports","ContactCards","Users","StoryHeads","Stories","FriendAliases","ArchiveReaders"]:
        delete_tree(db.document(root+"/"+uid))
    # Preserve a new owner's family grant if the address has been reassigned.
    family = db.document("FamilyAccess/"+email.lower())
    public = db.document("PublicInfo/removals")
    @firestore.transactional
    def remove_addresses(tx):
        f=family.get(transaction=tx).to_dict() or {}
        p=public.get(transaction=tx).to_dict() or {}
        if f.get("uid")==uid or (f.get("status")=="pending" and f.get("createdAt") and f["createdAt"]<=requested_at):
            tx.delete(family)
        if p.get("email","").lower()==email.lower():
            tx.delete(public)
    remove_addresses(db.transaction())

def process(job):
    uid=job.id
    d=job.to_dict()
    if "finishedAt" in d:
        if datetime.now(timezone.utc)-d["finishedAt"]>timedelta(minutes=65):
            job.reference.delete()
        return
    try:
        account=auth.get_user(uid)
    except auth.UserNotFoundError:
        account=None
    email=d["email"]
    # Never operate on another account if an old queue item is accidentally restored.
    if account and (account.email or "").lower()!=email.lower():
        raise RuntimeError("Account identity changed; review the request")
    if (db.document("AppConfig/moderation").get().to_dict() or {}).get("adminUid")==uid:
        raise RuntimeError("Transfer family administration before retrying")
    if account and not account.disabled:
        auth.update_user(uid, disabled=True)
    if d.get("transferGroups") is True:
        transfer_administered_groups(uid)
    erase(uid,email,d["requestedAt"])
    if account:
        auth.delete_user(uid)
    # Keep only a temporary deny marker while previously issued tokens expire.
    job.reference.set({"finishedAt":firestore.SERVER_TIMESTAMP})

def run():
    global db
    key=json.loads(os.environ["DNF_ERASER_SERVICE_ACCOUNT"])
    if key["project_id"]!=PROJECT:
        raise RuntimeError("Wrong Firebase project")
    firebase_admin.initialize_app(credentials.Certificate(key))
    db=firestore.client()
    failed=False
    for job in db.collection("GroupErasures").stream():
        try:
            group = db.document("Chats/" + job.id)
            data = group.get().to_dict()
            if data is not None and (data.get("members") != [] or data.get("adminUid") != ""):
                raise RuntimeError("Group still active")
            for invite in filtered(db.collection("Invites"), "chatId", "==", job.id).stream():
                invite.reference.delete()
            delete_tree(group)
            job.reference.delete()
        except Exception as exc:
            print("Group deletion pending:", type(exc).__name__, flush=True)
            failed=True
    for job in db.collection("AccountErasures").stream():
        try:
            process(job)
        except Exception as exc:
            print("Deletion pending:",type(exc).__name__,flush=True)
            failed=True
    if failed:
        raise RuntimeError("Review permissions, quotas or group ownership and run again")
    db.document("EraseService/status").set({"checkedAt":firestore.SERVER_TIMESTAMP, "groupDeletion":True, "groupSuccession":True})

if __name__=="__main__":
    run()
