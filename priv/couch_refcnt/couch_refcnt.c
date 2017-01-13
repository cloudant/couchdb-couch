#include <assert.h>
#include "erl_nif.h"


static ErlNifResourceType* COUCH_REFCNT_RES;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_CLOSED;
static ERL_NIF_TERM ATOM_NO_MATCH;
static ERL_NIF_TERM ATOM_REFCNT;


#ifndef ulong
typedef unsigned long ulong;
#endif

#ifndef uchar
typedef unsigned char uchar;
#endif


typedef struct
{
    ErlNifMutex*    lock;
    ErlNifEnv*      env;
    ERL_NIF_TERM    ref;
    ErlNifPid*      pid;
    ulong           count;
    uchar           closed;
} couch_refcnt_t;

typedef struct
{
    ErlNifMutex*        lock;
    couch_refcnt_t*     refcnt;
} couch_refcnt_handle_t;


static couch_refcnt_t*
couch_refcnt_init(ErlNifPid* pid)
{
    couch_refcnt_t* ret = (couch_refcnt_t*) enif_alloc(sizeof(couch_refcnt_t));

    ret->lock = enif_mutex_create("couch_refcnt_lock");
    ret->env = enif_alloc_env();
    ret->ref = enif_make_ref(ret->env);
    ret->count = 1;
    ret->closed = 0;

    if(pid == NULL) {
        ret->pid = NULL;
    } else {
        ret->pid = (ErlNifPid*) enif_alloc(sizeof(ErlNifPid));
        *(ret->pid) = *(pid);
    }

    return ret;
}


static void
couch_refcnt_destroy(couch_refcnt_t* refcnt)
{
    if(refcnt->pid != NULL) {
        enif_free(refcnt->pid);
    }

    enif_mutex_destroy(refcnt->lock);
    enif_free_env(refcnt->env);
    enif_free(refcnt);
}


static void
couch_refcnt_notify(couch_refcnt_t* refcnt)
{
    ErlNifEnv* msg_env;
    ERL_NIF_TERM msg_ref;
    ERL_NIF_TERM msg;

    if(refcnt->pid == NULL) {
        return;
    }

    msg_env = enif_alloc_env();
    msg_ref = enif_make_copy(msg_env, refcnt->ref);
    msg = enif_make_tuple2(msg_env, ATOM_REFCNT, msg_ref);

    enif_send(NULL, refcnt->pid, msg_env, msg);

    enif_free_env(msg_env);
}


static void
couch_refcnt_handle_free(ErlNifEnv* env, void* data)
{
    couch_refcnt_handle_t* handle = (couch_refcnt_handle_t*) data;

    enif_mutex_lock(handle->lock);

    if(handle->refcnt == NULL) {
        enif_mutex_unlock(handle->lock);
        enif_mutex_destroy(handle->lock);
        return;
    }

    enif_mutex_lock(handle->refcnt->lock);

    assert(handle->refcnt->count > 0 && "invalid ref count");
    handle->refcnt->count -= 1;

    if(handle->refcnt->count == 1) {
        couch_refcnt_notify(handle->refcnt);
    }

    if(handle->refcnt->count > 0) {
        enif_mutex_unlock(handle->refcnt->lock);
        goto done;
    }

    // Last ref to handle->refcnt is going away
    // so we have to free that resource.

    enif_mutex_unlock(handle->refcnt->lock);
    couch_refcnt_destroy(handle->refcnt);

done:

    enif_mutex_unlock(handle->lock);
    enif_mutex_destroy(handle->lock);
}


static ERL_NIF_TERM
couch_refcnt_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_refcnt_handle_t* handle;
    ErlNifPid pid;
    ERL_NIF_TERM ret;

    if(argc != 0 && argc != 1) {
        return enif_make_badarg(env);
    }

    if(argc == 1 && !enif_get_local_pid(env, argv[0], &pid)) {
        return enif_make_badarg(env);
    }

    handle = (couch_refcnt_handle_t*) enif_alloc_resource(
        COUCH_REFCNT_RES,
        sizeof(couch_refcnt_handle_t)
    );

    handle->lock = enif_mutex_create("couch_refcnt_handle_lock");
    if(argc == 1) {
        handle->refcnt = couch_refcnt_init(&pid);
    } else {
        handle->refcnt = couch_refcnt_init(NULL);
    }

    ret = enif_make_resource(env, handle);
    enif_release_resource(handle);
    return enif_make_tuple2(env, ATOM_OK, ret);
}


static ERL_NIF_TERM
couch_refcnt_incref(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_refcnt_handle_t* old_handle;
    couch_refcnt_handle_t* new_handle;
    ERL_NIF_TERM ret;
    void* tmp;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], COUCH_REFCNT_RES, (void**) &tmp)) {
        return enif_make_badarg(env);
    }

    old_handle = (couch_refcnt_handle_t*) tmp;

    enif_mutex_lock(old_handle->lock);

    if(old_handle->refcnt == NULL) {
        enif_mutex_unlock(old_handle->lock);
        return enif_make_badarg(env);
    }

    enif_mutex_lock(old_handle->refcnt->lock);

    if(old_handle->refcnt->closed) {
        enif_mutex_unlock(old_handle->refcnt->lock);
        enif_mutex_unlock(old_handle->lock);
        return ATOM_CLOSED;
    }

    old_handle->refcnt->count += 1;

    if(old_handle->refcnt->count == 2) {
        couch_refcnt_notify(old_handle->refcnt);
    }

    enif_mutex_unlock(old_handle->refcnt->lock);

    new_handle = (couch_refcnt_handle_t*) enif_alloc_resource(
            COUCH_REFCNT_RES,
            sizeof(couch_refcnt_handle_t)
        );
    new_handle->lock = enif_mutex_create("couch_refcnt_handle_lock");
    new_handle->refcnt = old_handle->refcnt;

    enif_mutex_unlock(old_handle->lock);

    ret = enif_make_resource(env, new_handle);
    ret = enif_make_tuple2(env, ATOM_OK, ret);
    enif_release_resource(new_handle);

    return ret;
}


static ERL_NIF_TERM
couch_refcnt_decref(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_refcnt_handle_t* handle;
    void* tmp;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], COUCH_REFCNT_RES, (void**) &tmp)) {
        return enif_make_badarg(env);
    }

    handle = (couch_refcnt_handle_t*) tmp;

    enif_mutex_lock(handle->lock);

    if(handle->refcnt == NULL) {
        enif_mutex_unlock(handle->lock);
        return enif_make_badarg(env);
    }

    enif_mutex_lock(handle->refcnt->lock);

    assert(handle->refcnt->count > 0 && "invalid refcount in decref");
    handle->refcnt->count -= 1;

    if(handle->refcnt->count == 1) {
        couch_refcnt_notify(handle->refcnt);
    }

    if(handle->refcnt->count > 0) {
        enif_mutex_unlock(handle->refcnt->lock);
        goto done;
    }

    enif_mutex_unlock(handle->refcnt->lock);
    couch_refcnt_destroy(handle->refcnt);

done:

    handle->refcnt = NULL;
    enif_mutex_unlock(handle->lock);

    return ATOM_OK;
}


static ERL_NIF_TERM
couch_refcnt_close_if(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_refcnt_handle_t* handle;
    ulong expect;
    ERL_NIF_TERM ret;
    void* tmp;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], COUCH_REFCNT_RES, (void**) &tmp)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_ulong(env, argv[1], &expect)) {
        return enif_make_badarg(env);
    }

    handle = (couch_refcnt_handle_t*) tmp;

    enif_mutex_lock(handle->lock);

    if(handle->refcnt == NULL) {
        enif_mutex_unlock(handle->lock);
        return enif_make_badarg(env);
    }

    enif_mutex_lock(handle->refcnt->lock);

    if(handle->refcnt->closed) {
        ret = ATOM_CLOSED;
    } else if(handle->refcnt->count == expect) {
        handle->refcnt->closed = 1;
        ret = ATOM_OK;
    } else {
        ret = ATOM_NO_MATCH;
    }

    enif_mutex_unlock(handle->refcnt->lock);
    enif_mutex_unlock(handle->lock);

    return ret;
}


static ERL_NIF_TERM
couch_refcnt_get_ref(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_refcnt_handle_t* handle;
    void* tmp;
    ERL_NIF_TERM ref;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], COUCH_REFCNT_RES, (void**) &tmp))
    {
        return enif_make_badarg(env);
    }

    handle = (couch_refcnt_handle_t*) tmp;

    enif_mutex_lock(handle->lock);

    if(handle->refcnt == NULL) {
        enif_mutex_unlock(handle->lock);
        return enif_make_badarg(env);
    }

    enif_mutex_lock(handle->refcnt->lock);
    ref = enif_make_copy(env, handle->refcnt->ref);
    enif_mutex_unlock(handle->refcnt->lock);

    enif_mutex_unlock(handle->lock);

    return enif_make_tuple2(env, ATOM_OK, ref);
}


static ERL_NIF_TERM
couch_refcnt_get_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_refcnt_handle_t* handle;
    void* tmp;
    ulong count;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], COUCH_REFCNT_RES, (void**) &tmp)) {
        return enif_make_badarg(env);
    }

    handle = (couch_refcnt_handle_t*) tmp;

    enif_mutex_lock(handle->lock);

    if(handle->refcnt == NULL) {
        enif_mutex_unlock(handle->lock);
        return enif_make_badarg(env);
    }

    enif_mutex_lock(handle->refcnt->lock);
    count = handle->refcnt->count;
    enif_mutex_unlock(handle->refcnt->lock);

    enif_mutex_unlock(handle->lock);

    return enif_make_tuple2(env, ATOM_OK, enif_make_ulong(env, count));
}


static ErlNifFunc nif_funcs[] =
{
    {"create", 0, couch_refcnt_create},
    {"create", 1, couch_refcnt_create},
    {"incref", 1, couch_refcnt_incref},
    {"decref", 1, couch_refcnt_decref},
    {"close_if", 2, couch_refcnt_close_if},
    {"get_ref", 1, couch_refcnt_get_ref},
    {"get_count", 1, couch_refcnt_get_count}
};


int
on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    COUCH_REFCNT_RES = enif_open_resource_type(
            env,
            NULL,
            "couch_refcnt_handle_res",
            couch_refcnt_handle_free,
            ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
            NULL
        );

    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_CLOSED = enif_make_atom(env, "closed");
    ATOM_NO_MATCH = enif_make_atom(env, "no_match");
    ATOM_REFCNT = enif_make_atom(env, "refcnt");

    return 0;
}

ERL_NIF_INIT(couch_refcnt, nif_funcs, &on_load, NULL, NULL, NULL);
