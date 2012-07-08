/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Ruby */
#include <ruby.h>

/* HyperDex */
#include "macros.h"

/* HyperClient */
#include "hyperclient/hyperclient.h"
#include "hyperclient/ruby/type_conversion.h"
#include "hyperdex.h"

static VALUE chyperclient;
static VALUE cexcept;
static VALUE cdeferred;
static VALUE cdeferred_get;
static VALUE cdeferred_from_attrs;
static VALUE cdeferred_condput;
static VALUE cdeferred_del;
static VALUE cdeferred_map_op;

/******************************* Error Handling *******************************/

#define RHC_ERROR_CASE(STATUS, DESCRIPTION) \
    case STATUS: \
        exception = rb_exc_new2(cexcept, DESCRIPTION); \
        rb_iv_set(exception, "@status", rb_uint_new(status)); \
        rb_iv_set(exception, "@symbol", rb_str_new2(hdxstr(STATUS))); \
        break

void
rhc_throw_hyperclient_exception(enum hyperclient_returncode status, const char* attr)
{
    VALUE exception = Qnil;
    const char* real_attr = attr == NULL ? "" : attr;
    char buf[2048];
    size_t num = 2047;

    switch (status)
    {
        RHC_ERROR_CASE(HYPERCLIENT_SUCCESS, "Success");
        RHC_ERROR_CASE(HYPERCLIENT_NOTFOUND, "Not Found");
        RHC_ERROR_CASE(HYPERCLIENT_SEARCHDONE, "Search Done");
        RHC_ERROR_CASE(HYPERCLIENT_CMPFAIL, "Conditional Operation Did Not Match Object");
        RHC_ERROR_CASE(HYPERCLIENT_READONLY, "Cluster is in a Read-Only State");
        RHC_ERROR_CASE(HYPERCLIENT_UNKNOWNSPACE, "Unknown Space");
        RHC_ERROR_CASE(HYPERCLIENT_COORDFAIL, "Coordinator Failure");
        RHC_ERROR_CASE(HYPERCLIENT_SERVERERROR, "Server Error");
        RHC_ERROR_CASE(HYPERCLIENT_POLLFAILED, "Polling Failed");
        RHC_ERROR_CASE(HYPERCLIENT_OVERFLOW, "Integer-overflow or divide-by-zero");
        RHC_ERROR_CASE(HYPERCLIENT_RECONFIGURE, "Reconfiguration");
        RHC_ERROR_CASE(HYPERCLIENT_TIMEOUT, "Timeout");
        case HYPERCLIENT_UNKNOWNATTR:
            num = snprintf(buf, 2047, "Unknown attribute \"%s\"", real_attr);
            num = num > 2047 ? 2047 : num;
            buf[num] = '\0';
            exception = rb_exc_new2(cexcept, buf);
            rb_iv_set(exception, "@status", rb_uint_new(status));
            rb_iv_set(exception, "@symbol", rb_str_new2("HYPERCLIENT_UNKNOWNATTR"));
            break;
        case HYPERCLIENT_DUPEATTR:
            num = snprintf(buf, 2047, "Duplicate attribute \"%s\"", real_attr);
            num = num > 2047 ? 2047 : num;
            buf[num] = '\0';
            exception = rb_exc_new2(cexcept, buf);
            rb_iv_set(exception, "@status", rb_uint_new(status));
            rb_iv_set(exception, "@symbol", rb_str_new2("HYPERCLIENT_DUPEATTR"));
            break;
        RHC_ERROR_CASE(HYPERCLIENT_NONEPENDING, "None pending");
        RHC_ERROR_CASE(HYPERCLIENT_DONTUSEKEY, "Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
        case HYPERCLIENT_WRONGTYPE:
            num = snprintf(buf, 2047, "Attribute \"%s\" has the wrong type", real_attr);
            num = num > 2047 ? 2047 : num;
            buf[num] = '\0';
            exception = rb_exc_new2(cexcept, buf);
            rb_iv_set(exception, "@status", rb_uint_new(status));
            rb_iv_set(exception, "@symbol", rb_str_new2("HYPERCLIENT_WRONGTYPE"));
            break;
        RHC_ERROR_CASE(HYPERCLIENT_NOMEM, "Memory allocation failed");
        RHC_ERROR_CASE(HYPERCLIENT_EXCEPTION, "Internal Error (file a bug)");
        default:
            exception = rb_exc_new2(cexcept, "Unknown Error (file a bug)");
            rb_iv_set(exception, "@status", rb_uint_new(status));
            rb_iv_set(exception, "@symbol", rb_str_new2("BUG"));
            break;
    }

    rb_exc_raise(exception);
}

#undef RHC_ERROR_CASE

/****************************** Check Request ID ******************************/

static void
check_reqid(int64_t reqid, enum hyperclient_returncode status)
{
    if (reqid < 0)
    {
        rhc_throw_hyperclient_exception(status, "");
    }
}

static void
check_reqid_key_attrs(int64_t reqid, enum hyperclient_returncode status,
                      struct hyperclient_attribute* attrs, size_t attrs_sz)
{
    ssize_t idx = 0;
    const char* attr = "";

    if (reqid < 0)
    {
        idx = -2 - reqid;

        if (idx >= 0 && idx < attrs_sz && attrs)
        {
            attr = attrs[idx].attr;
        }

        rhc_throw_hyperclient_exception(status, attr);
    }
}

static void
check_reqid_key_attrs2(int64_t reqid, enum hyperclient_returncode status,
                       struct hyperclient_attribute* attrs1, size_t attrs_sz1,
                       struct hyperclient_attribute* attrs2, size_t attrs_sz2)
{
    ssize_t idx = 0;
    const char* attr = "";

    if (reqid < 0)
    {
        idx = -2 - reqid;

        if (idx >= 0 && idx < attrs_sz1 && attrs1)
        {
            attr = attrs1[idx].attr;
        }

        idx -= attrs_sz1;

        if (idx >= 0 && idx < attrs_sz2 && attrs2)
        {
            attr = attrs2[idx].attr;
        }

        rhc_throw_hyperclient_exception(status, attr);
    }
}

#if 0
cdef _check_reqid_key_map_attrs(int64_t reqid, hyperclient_returncode status,
                                hyperclient_map_attribute* attrs, size_t attrs_sz):
    cdef bytes attr
    if reqid < 0:
        idx = -2 - reqid
        attr = None
        if idx >= 0 and idx < attrs_sz and attrs and attrs[idx].attr:
            attr = attrs[idx].attr
        raise HyperClientException(status, attr)


cdef _check_reqid_search(int64_t reqid, hyperclient_returncode status,
                         hyperclient_attribute* eq, size_t eq_sz,
                         hyperclient_range_query* rn, size_t rn_sz):
    cdef bytes attr
    if reqid < 0:
        idx = -1 - reqid
        attr = None
        if idx >= 0 and idx < eq_sz and eq and eq[idx].attr:
            attr = eq[idx].attr
        idx -= eq_sz
        if idx >= 0 and idx < rn_sz and rn and rn[idx].attr:
            attr = rn[idx].attr
        raise HyperClientException(status, attr)
#endif

/******************************* Deferred Class *******************************/

#define RHC_DEFERRED \
    VALUE client; \
    int64_t reqid; \
    enum hyperclient_returncode status; \
    int finished \

struct rhc_deferred
{
    RHC_DEFERRED;
};

void
rhc_deferred_mark(struct rhc_deferred* tom)
{
    if (tom)
    {
        rb_gc_mark(tom->client);
    }
}

void
rhc_deferred_free(struct rhc_deferred* tom)
{
    if (tom)
    {
        free(tom);
    }
}

VALUE
rhc_deferred_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred* d = malloc(sizeof(struct rhc_deferred));

    if (!d)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(d, 0, sizeof(struct rhc_deferred));
    d->client = Qnil;
    d->reqid = 0;
    d->status = HYPERCLIENT_ZERO;
    d->finished = 0;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_free, d);
    return tdata;
}

VALUE
rhc_deferred_init(VALUE self, VALUE client)
{
    struct rhc_deferred* d = NULL;
    Data_Get_Struct(self, struct rhc_deferred, d);
    d->client = client;
    return self;
}

VALUE
rhc_deferred_callback(VALUE self)
{
    VALUE ops;
    struct rhc_deferred* d = NULL;
    Data_Get_Struct(self, struct rhc_deferred, d);
    d->finished = 1;
    ops = rb_iv_get(d->client, "ops");
    rb_hash_delete(ops, INT2NUM(d->reqid));
    return Qnil;
}

VALUE
rhc_deferred_wait(VALUE self)
{
    VALUE from_loop;
    struct rhc_deferred* d = NULL;
    Data_Get_Struct(self, struct rhc_deferred, d);

    while (!d->finished && d->reqid > 0)
    {
        from_loop = rb_funcall(d->client, rb_intern("loop"), 0);

        if (from_loop == Qnil)
        {
            return Qnil;
        }
    }

    d->finished = 1;
    return Qnil;
}

/****************************** DeferredGet Class *****************************/

struct rhc_deferred_get
{
    RHC_DEFERRED;
    struct hyperclient_attribute* attrs;
    size_t attrs_sz;
};

void
rhc_deferred_get_free(struct rhc_deferred_get* tom)
{
    if (tom)
    {
        if (tom->attrs)
        {
            hyperclient_destroy_attrs(tom->attrs, tom->attrs_sz);
        }

        free(tom);
    }
}

VALUE
rhc_deferred_get_new(VALUE client, VALUE space, VALUE key)
{
    VALUE argv[3];
    argv[0] = client;
    argv[1] = space;
    argv[2] = key;
    return rb_class_new_instance(3, argv, cdeferred_get);
}

VALUE
rhc_deferred_get_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred_get* d = malloc(sizeof(struct rhc_deferred_get));

    if (!d)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(d, 0, sizeof(struct rhc_deferred_get));
    d->client = Qnil;
    d->reqid = 0;
    d->status = HYPERCLIENT_ZERO;
    d->finished = 0;
    d->attrs = NULL;
    d->attrs_sz = 0;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_get_free, d);
    return tdata;
}

VALUE
rhc_deferred_get_init(VALUE self, VALUE client, VALUE space, VALUE key)
{
    struct rhc_deferred_get* d = NULL;
    struct hyperclient* c = NULL;
    enum hyperdatatype datatype;
    VALUE key_backing;
    const char* space_cstr;
    const char* key_cstr;
    size_t key_sz;
    VALUE ops;

    rb_call_super(1, &client);
    hyperclient_ruby_obj_to_backing(key, &datatype, &key_backing);
    space_cstr = StringValueCStr(space);
    key_cstr = rb_str2cstr(key, &key_sz);
    Data_Get_Struct(self, struct rhc_deferred_get, d);
    Data_Get_Struct(d->client, struct hyperclient, c);
    d->reqid = hyperclient_get(c, space_cstr, key_cstr, key_sz,
                               &d->status, &d->attrs, &d->attrs_sz);
    check_reqid(d->reqid, d->status);
    ops = rb_iv_get(d->client, "ops");
    rb_hash_aset(ops, INT2NUM(d->reqid), self);
    return self;
}

VALUE
rhc_deferred_get_wait(VALUE self)
{
    struct rhc_deferred_get* d = NULL;

    rb_call_super(0, NULL);
    Data_Get_Struct(self, struct rhc_deferred_get, d);

    if (d->status == HYPERCLIENT_SUCCESS)
    {
        return hyperclient_ruby_attrs_to_hash(d->attrs, d->attrs_sz);
    }
    else if (d->status == HYPERCLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else
    {
        rhc_throw_hyperclient_exception(d->status, "");
    }
}

/*************************** DeferredFromAttrs Class **************************/

typedef int64_t (*hyperclient_simple_op)(struct hyperclient*,
                                         const char*, const char*, size_t,
                                         const struct hyperclient_attribute*, size_t,
                                         enum hyperclient_returncode*);

struct rhc_deferred_from_attrs
{
    RHC_DEFERRED;
    hyperclient_simple_op op;
    int cmped;
};

void
rhc_deferred_from_attrs_free(struct rhc_deferred_from_attrs* tom)
{
    if (tom)
    {
        free(tom);
    }
}

VALUE
rhc_deferred_from_attrs_new(VALUE client, VALUE space, VALUE key, VALUE value,
                            hyperclient_simple_op op)
{
    VALUE argv[5];
    argv[0] = client;
    argv[1] = space;
    argv[2] = key;
    argv[3] = value;
    argv[4] = Data_Wrap_Struct(rb_cObject, NULL, NULL, op);
    return rb_class_new_instance(5, argv, cdeferred_from_attrs);
}

VALUE
rhc_deferred_from_attrs_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred_from_attrs* d = malloc(sizeof(struct rhc_deferred_from_attrs));

    if (!d)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(d, 0, sizeof(struct rhc_deferred_from_attrs));
    d->client = Qnil;
    d->reqid = 0;
    d->status = HYPERCLIENT_ZERO;
    d->finished = 0;
    d->op = NULL;
    d->cmped = 0;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_from_attrs_free, d);
    return tdata;
}

VALUE
rhc_deferred_from_attrs_init(VALUE self, VALUE client, VALUE space, VALUE key, VALUE value, VALUE op)
{
    struct rhc_deferred_from_attrs* d = NULL;
    struct hyperclient* c = NULL;
    enum hyperdatatype datatype = HYPERDATATYPE_GARBAGE;
    VALUE key_backing = Qnil;
    const char* space_cstr = NULL;
    const char* key_cstr = NULL;
    size_t key_sz = 0;
    VALUE backing = Qnil;
    struct hyperclient_attribute* attrs = NULL;
    size_t attrs_sz = 0;
    VALUE ops = Qnil;

    rb_call_super(1, &client);
    hyperclient_ruby_obj_to_backing(key, &datatype, &key_backing);
    space_cstr = StringValueCStr(space);
    key_cstr = rb_str2cstr(key, &key_sz);
    hyperclient_ruby_hash_to_attrs(value, &backing, &attrs, &attrs_sz);
    Data_Get_Struct(self, struct rhc_deferred_from_attrs, d);
    Data_Get_Struct(d->client, struct hyperclient, c);
    Data_Get_Struct(op, hyperclient_simple_op, d->op);
    d->reqid = d->op(c, space_cstr, key_cstr, key_sz, attrs, attrs_sz, &d->status);
    check_reqid_key_attrs(d->reqid, d->status, attrs, attrs_sz);
    ops = rb_iv_get(d->client, "ops");
    rb_hash_aset(ops, INT2NUM(d->reqid), self);
    return self;
}

VALUE
rhc_deferred_from_attrs_wait(VALUE self)
{
    struct rhc_deferred_from_attrs* d = NULL;

    rb_call_super(0, NULL);
    Data_Get_Struct(self, struct rhc_deferred_from_attrs, d);

    if (d->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    else if (d->cmped && d->status == HYPERCLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        rhc_throw_hyperclient_exception(d->status, "");
    }
}

/**************************** DeferredCondPut Class ***************************/

struct rhc_deferred_condput
{
    RHC_DEFERRED;
};

void
rhc_deferred_condput_free(struct rhc_deferred_from_attrs* tom)
{
    if (tom)
    {
        free(tom);
    }
}

VALUE
rhc_deferred_condput_new(VALUE client, VALUE space, VALUE key, VALUE condition, VALUE value)
{
    VALUE argv[5];
    argv[0] = client;
    argv[1] = space;
    argv[2] = key;
    argv[3] = condition;
    argv[4] = value;
    return rb_class_new_instance(5, argv, cdeferred_condput);
}

VALUE
rhc_deferred_condput_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred_condput* d = malloc(sizeof(struct rhc_deferred_condput));

    if (!d)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(d, 0, sizeof(struct rhc_deferred_condput));
    d->client = Qnil;
    d->reqid = 0;
    d->status = HYPERCLIENT_ZERO;
    d->finished = 0;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_condput_free, d);
    return tdata;
}

VALUE
rhc_deferred_condput_init(VALUE self, VALUE client, VALUE space, VALUE key, VALUE condition, VALUE value)
{
    struct rhc_deferred_condput* d = NULL;
    struct hyperclient* c = NULL;
    enum hyperdatatype datatype = HYPERDATATYPE_GARBAGE;
    VALUE key_backing = Qnil;
    const char* space_cstr = NULL;
    const char* key_cstr = NULL;
    size_t key_sz = 0;
    VALUE backinga = Qnil;
    VALUE backingc = Qnil;
    struct hyperclient_attribute* attrs = NULL;
    size_t attrs_sz = 0;
    struct hyperclient_attribute* condattrs = NULL;
    size_t condattrs_sz = 0;
    VALUE ops = Qnil;

    rb_call_super(1, &client);
    hyperclient_ruby_obj_to_backing(key, &datatype, &key_backing);
    space_cstr = StringValueCStr(space);
    key_cstr = rb_str2cstr(key, &key_sz);
    hyperclient_ruby_hash_to_attrs(value, &backinga, &attrs, &attrs_sz);
    hyperclient_ruby_hash_to_attrs(condition, &backingc, &condattrs, &condattrs_sz);
    Data_Get_Struct(self, struct rhc_deferred_condput, d);
    Data_Get_Struct(d->client, struct hyperclient, c);
    d->reqid = hyperclient_condput(c, space_cstr, key_cstr, key_sz, condattrs, condattrs_sz, attrs, attrs_sz, &d->status);
    check_reqid_key_attrs2(d->reqid, d->status, condattrs, condattrs_sz, attrs, attrs_sz);
    ops = rb_iv_get(d->client, "ops");
    rb_hash_aset(ops, INT2NUM(d->reqid), self);
    return self;
}

VALUE
rhc_deferred_condput_wait(VALUE self)
{
    struct rhc_deferred_condput* d = NULL;

    rb_call_super(0, NULL);
    Data_Get_Struct(self, struct rhc_deferred_condput, d);

    if (d->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    else if (d->status == HYPERCLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        rhc_throw_hyperclient_exception(d->status, "");
    }
}

/**************************** DeferredDel Class ****************************/

struct rhc_deferred_del
{
    RHC_DEFERRED;
};

void
rhc_deferred_del_free(struct rhc_deferred_del* tom)
{
    if (tom)
    {
        free(tom);
    }
}

VALUE
rhc_deferred_del_new(VALUE client, VALUE space, VALUE key)
{
    VALUE argv[3];
    argv[0] = client;
    argv[1] = space;
    argv[2] = key;
    return rb_class_new_instance(4, argv, cdeferred_del);
}

VALUE
rhc_deferred_del_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred_del* d = malloc(sizeof(struct rhc_deferred_del));

    if (!d)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(d, 0, sizeof(struct rhc_deferred_del));
    d->client = Qnil;
    d->reqid = 0;
    d->status = HYPERCLIENT_ZERO;
    d->finished = 0;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_del_free, d);
    return tdata;
}

VALUE
rhc_deferred_del_init(VALUE self, VALUE client, VALUE space, VALUE key, VALUE value)
{
    struct rhc_deferred_del* d = NULL;
    struct hyperclient* c = NULL;
    enum hyperdatatype datatype = HYPERDATATYPE_GARBAGE;
    VALUE key_backing = Qnil;
    const char* space_cstr = NULL;
    const char* key_cstr = NULL;
    size_t key_sz = 0;
    VALUE ops = Qnil;

    rb_call_super(1, &client);
    hyperclient_ruby_obj_to_backing(key, &datatype, &key_backing);
    space_cstr = StringValueCStr(space);
    key_cstr = rb_str2cstr(key, &key_sz);
    Data_Get_Struct(self, struct rhc_deferred_del, d);
    Data_Get_Struct(d->client, struct hyperclient, c);
    d->reqid = hyperclient_del(c, space_cstr, key_cstr, key_sz, &d->status);
    check_reqid(d->reqid, d->status);
    ops = rb_iv_get(d->client, "ops");
    rb_hash_aset(ops, INT2NUM(d->reqid), self);
    return self;
}

VALUE
rhc_deferred_del_wait(VALUE self)
{
    struct rhc_deferred_del* d = NULL;

    rb_call_super(0, NULL);
    Data_Get_Struct(self, struct rhc_deferred_del, d);

    if (d->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    if (d->status == HYPERCLIENT_NOTFOUND)
    {
        return Qfalse;
    }
    else
    {
        rhc_throw_hyperclient_exception(d->status, "");
    }
}

/***************************** DeferredMapOp Class ****************************/

typedef int64_t (*hyperclient_map_op)(struct hyperclient*,
                                      const char*, const char*, size_t,
                                      const struct hyperclient_map_attribute*, size_t,
                                      enum hyperclient_returncode*);

struct rhc_deferred_map_op
{
    RHC_DEFERRED;
    hyperclient_map_op op;
};

void
rhc_deferred_map_op_free(struct rhc_deferred_map_op* tom)
{
    if (tom)
    {
        free(tom);
    }
}

VALUE
rhc_deferred_map_op_new(VALUE client, VALUE space, VALUE key, VALUE value, hyperclient_map_op op)
{
    VALUE argv[5];
    argv[0] = client;
    argv[1] = space;
    argv[2] = key;
    argv[3] = value;
    argv[4] = Data_Wrap_Struct(rb_cObject, NULL, NULL, op);
    return rb_class_new_instance(5, argv, cdeferred_map_op);
}

VALUE
rhc_deferred_map_op_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred_map_op* d = malloc(sizeof(struct rhc_deferred_map_op));

    if (!d)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(d, 0, sizeof(struct rhc_deferred_map_op));
    d->client = Qnil;
    d->reqid = 0;
    d->status = HYPERCLIENT_ZERO;
    d->finished = 0;
    d->op = NULL;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_map_op_free, d);
    return tdata;
}

VALUE
rhc_deferred_map_op_init(VALUE self, VALUE client, VALUE space, VALUE key, VALUE value, VALUE op)
{
    struct rhc_deferred_map_op* d = NULL;
    struct hyperclient* c = NULL;
    enum hyperdatatype datatype = HYPERDATATYPE_GARBAGE;
    VALUE key_backing = Qnil;
    const char* space_cstr = NULL;
    const char* key_cstr = NULL;
    size_t key_sz = 0;
    VALUE backing = Qnil;
    struct hyperclient_map_attribute* attrs = NULL;
    size_t attrs_sz = 0;
    VALUE ops = Qnil;

    rb_call_super(1, &client);
    hyperclient_ruby_obj_to_backing(key, &datatype, &key_backing);
    space_cstr = StringValueCStr(space);
    key_cstr = rb_str2cstr(key, &key_sz);
    hyperclient_ruby_hash_to_map_attrs(value, &backing, &attrs, &attrs_sz);
    Data_Get_Struct(self, struct rhc_deferred_map_op, d);
    Data_Get_Struct(d->client, struct hyperclient, c);
    Data_Get_Struct(op, hyperclient_map_op, d->op);
    d->reqid = d->op(c, space_cstr, key_cstr, key_sz, attrs, attrs_sz, &d->status);
    check_reqid_key_map_attrs(d->reqid, d->status, attrs, attrs_sz);
    ops = rb_iv_get(d->client, "ops");
    rb_hash_aset(ops, INT2NUM(d->reqid), self);
    return self;
}

VALUE
rhc_deferred_map_op_wait(VALUE self)
{
    struct rhc_deferred_map_op* d = NULL;

    rb_call_super(0, NULL);
    Data_Get_Struct(self, struct rhc_deferred_map_op, d);

    if (d->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    else
    {
        rhc_throw_hyperclient_exception(d->status, "");
    }
}

/****************************** HyperClient Class *****************************/

VALUE
rhc_new(VALUE class, VALUE host, VALUE port)
{
    VALUE argv[2];
    VALUE tdata;
    struct hyperclient* hc = hyperclient_create(StringValueCStr(host), NUM2UINT(port));

    if (!hc)
    {
        rb_raise(rb_eSystemCallError, "HyperClient connect failed");
        return Qnil;
    }

    tdata = Data_Wrap_Struct(class, 0, hyperclient_destroy, hc);
    argv[0] = host;
    argv[1] = port;
    rb_obj_call_init(tdata, 2, argv);
    return tdata;
}

VALUE
rhc_init(VALUE self, VALUE host, VALUE port)
{
    VALUE ops;
    ops = rb_hash_new();
    if (ops == Qnil) return Qnil;
    rb_iv_set(self, "ops", ops);
    return self;
}

#define RHC_SYNC0(X) \
    VALUE \
    rhc_ ## X(VALUE self, VALUE space, VALUE key) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" hdxstr(X)), 2, space, key); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    }
#define RHC_SYNC1(X) \
    VALUE \
    rhc_ ## X(VALUE self, VALUE space, VALUE key, VALUE value1) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" hdxstr(X)), 3, space, key, value1); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    }
#define RHC_SYNC2(X) \
    VALUE \
    rhc_ ## X(VALUE self, VALUE space, VALUE key, VALUE value1, VALUE value2) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" hdxstr(X)), 4, space, key, value1, value2); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    }

RHC_SYNC0(get)
RHC_SYNC1(put)
RHC_SYNC1(put_if_not_exist)
RHC_SYNC2(condput)
RHC_SYNC0(del)
RHC_SYNC1(atomic_add)
RHC_SYNC1(atomic_sub)
RHC_SYNC1(atomic_mul)
RHC_SYNC1(atomic_div)
RHC_SYNC1(atomic_mod)
RHC_SYNC1(atomic_and)
RHC_SYNC1(atomic_or)
RHC_SYNC1(atomic_xor)
RHC_SYNC1(string_prepend)
RHC_SYNC1(string_append)
RHC_SYNC1(list_lpush)
RHC_SYNC1(list_rpush)
RHC_SYNC1(set_add)
RHC_SYNC1(set_remove)
RHC_SYNC1(set_intersect)
RHC_SYNC1(set_union)
RHC_SYNC1(map_add)
RHC_SYNC1(map_remove)
RHC_SYNC1(map_atomic_add)
RHC_SYNC1(map_atomic_sub)
RHC_SYNC1(map_atomic_mul)
RHC_SYNC1(map_atomic_div)
RHC_SYNC1(map_atomic_mod)
RHC_SYNC1(map_atomic_and)
RHC_SYNC1(map_atomic_or)
RHC_SYNC1(map_atomic_xor)
RHC_SYNC1(map_string_prepend)
RHC_SYNC1(map_string_append)
RHC_SYNC0(group_del)
RHC_SYNC1(count)

// XXX
// def search(self, bytes space, dict predicate):
//     return Search(self, space, predicate)
//
// def sorted_search(self, bytes space, dict predicate, bytes sort_by, long limit, bytes compare):
//     return SortedSearch(self, space, predicate, sort_by, limit, compare)

#define RHC_ASYNC0_NOFP(X, D) \
    VALUE \
    rhc_async_ ## X(VALUE self, VALUE space, VALUE key) \
    { \
        return D(self, space, key); \
    }

#define RHC_ASYNC2_NOFP(X, D) \
    VALUE \
    rhc_async_ ## X(VALUE self, VALUE space, VALUE key, VALUE value1, VALUE value2) \
    { \
        return D(self, space, key, value1, value2); \
    }

#define RHC_ASYNC_FROM_ATTRS(X) \
    VALUE \
    rhc_async_ ## X(VALUE self, VALUE space, VALUE key, VALUE value) \
    { \
        VALUE d; \
        d = rhc_deferred_from_attrs_new(self, space, key, value, hyperclient_ ## X); \
        return d; \
    }

#define RHC_ASYNC_MAP_OP(X) \
    VALUE \
    rhc_async_ ## X(VALUE self, VALUE space, VALUE key, VALUE value) \
    { \
        VALUE d; \
        d = rhc_deferred_map_op_new(self, space, key, value, hyperclient_ ## X); \
        return d; \
    }

RHC_ASYNC0_NOFP(get, rhc_deferred_get_new)
RHC_ASYNC_FROM_ATTRS(put)
VALUE
rhc_async_put_if_not_exist(VALUE self, VALUE space, VALUE key, VALUE value)
{
    VALUE d;
    struct rhc_deferred_from_attrs* p;
    d = rhc_deferred_from_attrs_new(self, space, key, value, hyperclient_put_if_not_exist);
    Data_Get_Struct(d, struct rhc_deferred_from_attrs, p);
    p->cmped = 1;
    return d;
}
RHC_ASYNC2_NOFP(condput, rhc_deferred_condput_new)
RHC_ASYNC0_NOFP(del, rhc_deferred_del_new)

RHC_ASYNC_FROM_ATTRS(atomic_add)
RHC_ASYNC_FROM_ATTRS(atomic_sub)
RHC_ASYNC_FROM_ATTRS(atomic_mul)
RHC_ASYNC_FROM_ATTRS(atomic_div)
RHC_ASYNC_FROM_ATTRS(atomic_mod)
RHC_ASYNC_FROM_ATTRS(atomic_and)
RHC_ASYNC_FROM_ATTRS(atomic_or)
RHC_ASYNC_FROM_ATTRS(atomic_xor)
RHC_ASYNC_FROM_ATTRS(string_prepend)
RHC_ASYNC_FROM_ATTRS(string_append)
RHC_ASYNC_FROM_ATTRS(list_lpush)
RHC_ASYNC_FROM_ATTRS(list_rpush)
RHC_ASYNC_FROM_ATTRS(set_add)
RHC_ASYNC_FROM_ATTRS(set_remove)
RHC_ASYNC_FROM_ATTRS(set_intersect)
RHC_ASYNC_FROM_ATTRS(set_union)

RHC_ASYNC_MAP_OP(map_add)
RHC_ASYNC_MAP_OP(map_remove)
RHC_ASYNC_MAP_OP(map_atomic_add)
RHC_ASYNC_MAP_OP(map_atomic_sub)
RHC_ASYNC_MAP_OP(map_atomic_mul)
RHC_ASYNC_MAP_OP(map_atomic_div)
RHC_ASYNC_MAP_OP(map_atomic_mod)
RHC_ASYNC_MAP_OP(map_atomic_and)
RHC_ASYNC_MAP_OP(map_atomic_or)
RHC_ASYNC_MAP_OP(map_atomic_xor)
RHC_ASYNC_MAP_OP(map_string_prepend)
RHC_ASYNC_MAP_OP(map_string_append)

//RHC_ASYNC(group_del, rhc_deferred_group_del_new)
//RHC_ASYNC(count, rhc_deferred_count_new)


VALUE
rhc_loop(VALUE self)
{
    struct hyperclient* c;
    enum hyperclient_returncode rc;
    int64_t ret;
    VALUE ops;
    VALUE op;

    Data_Get_Struct(self, struct hyperclient, c);
    ret = hyperclient_loop(c, -1, &rc);

    if (ret < 0)
    {
        rhc_throw_hyperclient_exception(rc, "");
    }
    else
    {
        ops = rb_iv_get(self, "ops");
        op = rb_hash_lookup(ops, INT2NUM(ret));
        rb_funcall(op, rb_intern("callback"), 0);
        return op;
    }
}

/****************************** Initialize Things *****************************/

void
Init_hyperclient()
{
    rb_require("set");

    chyperclient = rb_define_class("HyperClient", rb_cObject);
    rb_define_singleton_method(chyperclient, "new", rhc_new, 2);
    rb_define_method(chyperclient, "initialize", rhc_init, 2);
    rb_define_method(chyperclient, "loop", rhc_loop, 0);

    rb_define_method(chyperclient, "get", rhc_get, 2);
    rb_define_method(chyperclient, "put", rhc_put, 3);
    rb_define_method(chyperclient, "put_if_not_exist", rhc_put_if_not_exist, 3);
    rb_define_method(chyperclient, "condput", rhc_condput, 4);
    rb_define_method(chyperclient, "del", rhc_del, 2);
    rb_define_method(chyperclient, "atomic_add", rhc_atomic_add, 3);
    rb_define_method(chyperclient, "atomic_sub", rhc_atomic_sub, 3);
    rb_define_method(chyperclient, "atomic_mul", rhc_atomic_mul, 3);
    rb_define_method(chyperclient, "atomic_div", rhc_atomic_div, 3);
    rb_define_method(chyperclient, "atomic_mod", rhc_atomic_mod, 3);
    rb_define_method(chyperclient, "atomic_and", rhc_atomic_and, 3);
    rb_define_method(chyperclient, "atomic_or", rhc_atomic_or, 3);
    rb_define_method(chyperclient, "atomic_xor", rhc_atomic_xor, 3);
    rb_define_method(chyperclient, "string_prepend", rhc_string_prepend, 3);
    rb_define_method(chyperclient, "string_append", rhc_string_append, 3);
    rb_define_method(chyperclient, "list_lpush", rhc_list_lpush, 3);
    rb_define_method(chyperclient, "list_rpush", rhc_list_rpush, 3);
    rb_define_method(chyperclient, "set_add", rhc_set_add, 3);
    rb_define_method(chyperclient, "set_remove", rhc_set_remove, 3);
    rb_define_method(chyperclient, "set_intersect", rhc_set_intersect, 3);
    rb_define_method(chyperclient, "set_union", rhc_set_union, 3);
    rb_define_method(chyperclient, "map_add", rhc_map_add, 3);
    rb_define_method(chyperclient, "map_remove", rhc_map_remove, 3);
    rb_define_method(chyperclient, "map_atomic_add", rhc_map_atomic_add, 3);
    rb_define_method(chyperclient, "map_atomic_sub", rhc_map_atomic_sub, 3);
    rb_define_method(chyperclient, "map_atomic_mul", rhc_map_atomic_mul, 3);
    rb_define_method(chyperclient, "map_atomic_div", rhc_map_atomic_div, 3);
    rb_define_method(chyperclient, "map_atomic_mod", rhc_map_atomic_mod, 3);
    rb_define_method(chyperclient, "map_atomic_and", rhc_map_atomic_and, 3);
    rb_define_method(chyperclient, "map_atomic_or", rhc_map_atomic_or, 3);
    rb_define_method(chyperclient, "map_atomic_xor", rhc_map_atomic_xor, 3);
    rb_define_method(chyperclient, "map_string_prepend", rhc_map_string_prepend, 3);
    rb_define_method(chyperclient, "map_string_append", rhc_map_string_append, 3);
    rb_define_method(chyperclient, "group_del", rhc_group_del, 2);
    rb_define_method(chyperclient, "count", rhc_count, 3);

    rb_define_method(chyperclient, "async_get", rhc_async_get, 2);
    rb_define_method(chyperclient, "async_put", rhc_async_put, 3);
    rb_define_method(chyperclient, "async_put_if_not_exist", rhc_async_put_if_not_exist, 3);
    rb_define_method(chyperclient, "async_condput", rhc_async_condput, 4);
    rb_define_method(chyperclient, "async_del", rhc_async_del, 2);
    rb_define_method(chyperclient, "async_atomic_add", rhc_async_atomic_add, 3);
    rb_define_method(chyperclient, "async_atomic_sub", rhc_async_atomic_sub, 3);
    rb_define_method(chyperclient, "async_atomic_mul", rhc_async_atomic_mul, 3);
    rb_define_method(chyperclient, "async_atomic_div", rhc_async_atomic_div, 3);
    rb_define_method(chyperclient, "async_atomic_mod", rhc_async_atomic_mod, 3);
    rb_define_method(chyperclient, "async_atomic_and", rhc_async_atomic_and, 3);
    rb_define_method(chyperclient, "async_atomic_or", rhc_async_atomic_or, 3);
    rb_define_method(chyperclient, "async_atomic_xor", rhc_async_atomic_xor, 3);
    rb_define_method(chyperclient, "async_string_prepend", rhc_async_string_prepend, 3);
    rb_define_method(chyperclient, "async_string_append", rhc_async_string_append, 3);
    rb_define_method(chyperclient, "async_list_lpush", rhc_async_list_lpush, 3);
    rb_define_method(chyperclient, "async_list_rpush", rhc_async_list_rpush, 3);
    rb_define_method(chyperclient, "async_set_add", rhc_async_set_add, 3);
    rb_define_method(chyperclient, "async_set_remove", rhc_async_set_remove, 3);
    rb_define_method(chyperclient, "async_set_intersect", rhc_async_set_intersect, 3);
    rb_define_method(chyperclient, "async_set_union", rhc_async_set_union, 3);
    rb_define_method(chyperclient, "async_map_add", rhc_async_map_add, 3);
    rb_define_method(chyperclient, "async_map_remove", rhc_async_map_remove, 3);
    rb_define_method(chyperclient, "async_map_atomic_add", rhc_async_map_atomic_add, 3);
    rb_define_method(chyperclient, "async_map_atomic_sub", rhc_async_map_atomic_sub, 3);
    rb_define_method(chyperclient, "async_map_atomic_mul", rhc_async_map_atomic_mul, 3);
    rb_define_method(chyperclient, "async_map_atomic_div", rhc_async_map_atomic_div, 3);
    rb_define_method(chyperclient, "async_map_atomic_mod", rhc_async_map_atomic_mod, 3);
    rb_define_method(chyperclient, "async_map_atomic_and", rhc_async_map_atomic_and, 3);
    rb_define_method(chyperclient, "async_map_atomic_or", rhc_async_map_atomic_or, 3);
    rb_define_method(chyperclient, "async_map_atomic_xor", rhc_async_map_atomic_xor, 3);
    rb_define_method(chyperclient, "async_map_string_prepend", rhc_async_map_string_prepend, 3);
    rb_define_method(chyperclient, "async_map_string_append", rhc_async_map_string_append, 3);
    //rb_define_method(chyperclient, "async_group_del", rhc_async_group_del, 2);
    //rb_define_method(chyperclient, "async_count", rhc_async_count, 3);

    cexcept = rb_define_class_under(chyperclient, "HyperClientException", rb_eStandardError);
    rb_define_attr(cexcept, "status", 1, 0);
    rb_define_attr(cexcept, "symbol", 1, 0);

    cdeferred = rb_define_class_under(chyperclient, "Deferred", rb_cObject);
    rb_define_alloc_func(cdeferred, rhc_deferred_alloc);
    rb_define_method(cdeferred, "initialize", rhc_deferred_init, 1);
    rb_define_method(cdeferred, "callback", rhc_deferred_callback, 0);
    rb_define_method(cdeferred, "wait", rhc_deferred_wait, 0);

    cdeferred_get = rb_define_class_under(chyperclient, "DeferredGet", cdeferred);
    rb_define_alloc_func(cdeferred_get, rhc_deferred_get_alloc);
    rb_define_method(cdeferred_get, "initialize", rhc_deferred_get_init, 3);
    rb_define_method(cdeferred_get, "wait", rhc_deferred_get_wait, 0);

    cdeferred_from_attrs = rb_define_class_under(chyperclient, "DeferredFromAttrs", cdeferred);
    rb_define_alloc_func(cdeferred_from_attrs, rhc_deferred_from_attrs_alloc);
    rb_define_method(cdeferred_from_attrs, "initialize", rhc_deferred_from_attrs_init, 5);
    rb_define_method(cdeferred_from_attrs, "wait", rhc_deferred_from_attrs_wait, 0);

    cdeferred_condput = rb_define_class_under(chyperclient, "DeferredCondPut", cdeferred);
    rb_define_alloc_func(cdeferred_condput, rhc_deferred_condput_alloc);
    rb_define_method(cdeferred_condput, "initialize", rhc_deferred_condput_init, 5);
    rb_define_method(cdeferred_condput, "wait", rhc_deferred_condput_wait, 0);

    cdeferred_del = rb_define_class_under(chyperclient, "DeferredDel", cdeferred);
    rb_define_alloc_func(cdeferred_del, rhc_deferred_del_alloc);
    rb_define_method(cdeferred_del, "initialize", rhc_deferred_del_init, 4);
    rb_define_method(cdeferred_del, "wait", rhc_deferred_del_wait, 0);

    cdeferred_map_op = rb_define_class_under(chyperclient, "DeferredMapOp", cdeferred);
    rb_define_alloc_func(cdeferred_map_op, rhc_deferred_map_op_alloc);
    rb_define_method(cdeferred_map_op, "initialize", rhc_deferred_map_op_init, 5);
    rb_define_method(cdeferred_map_op, "wait", rhc_deferred_map_op_wait, 0);
}
