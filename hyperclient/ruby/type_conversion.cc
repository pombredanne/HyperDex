// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#include "macros.h"
#include <cstdio>

// ruby
#include <ruby.h>

// STL
#include <algorithm>

// e
#include <e/endian.h>

// HyperDex
#include "datatypes/compare.h"

// HyperClient
#include "hyperclient/ruby/type_conversion.h"

extern "C"
{

void
hyperclient_elem_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing);

}

static void
_symbol_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    const char* symb = rb_id2name(SYM2ID(obj));
    *datatype = HYPERDATATYPE_STRING;
    *backing = rb_str_new2(symb);
}

static void
_string_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    *datatype = HYPERDATATYPE_STRING;
    *backing = obj;
}

static void
_symbol_to_inner_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    const char* symb = rb_id2name(SYM2ID(obj));
    *datatype = HYPERDATATYPE_STRING;
    uint32_t sz = NUM2UINT(strlen(symb));
    char buf[sizeof(sz)];
    e::pack32le(sz, buf);
    *backing = rb_str_new(buf, sizeof(buf));
    rb_str_cat2(*backing, symb);
}

static void
_string_to_inner_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    *datatype = HYPERDATATYPE_STRING;
    uint32_t sz = RSTRING_LEN(obj);
    char buf[sizeof(sz)];
    e::pack32le(sz, buf);
    *backing = rb_str_new(buf, sizeof(buf));
    rb_str_append(*backing, obj);
}

static void
_num_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    int64_t val = NUM2LL(obj);
    char buf[sizeof(int64_t)];
    e::pack64le(val, buf);
    *datatype = HYPERDATATYPE_INT64;
    *backing = rb_str_new(buf, sizeof(int64_t));
}

static void
_float_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    double val = NUM2DBL(obj);
    char buf[sizeof(double)];
    e::packdoublele(val, buf);
    *datatype = HYPERDATATYPE_FLOAT;
    *backing = rb_str_new(buf, sizeof(double));
}

static void
_list_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    VALUE elem = Qnil;
    VALUE elem_backing = Qnil;
    enum hyperdatatype elem_datatype = HYPERDATATYPE_GARBAGE;
    enum hyperdatatype all_datatype = HYPERDATATYPE_GARBAGE;
    *backing = rb_str_new("", 0);

    for (ssize_t i = 0; i < RARRAY_LEN(obj); ++i)
    {
        elem = rb_ary_entry(obj, i);
        hyperclient_elem_to_backing(elem, &elem_datatype, &elem_backing);

        if (elem_backing == Qnil)
        {
            rb_raise(rb_eTypeError, "Do not know how to convert attribute");
        }

        if (elem_datatype != HYPERDATATYPE_STRING &&
            elem_datatype != HYPERDATATYPE_INT64 &&
            elem_datatype != HYPERDATATYPE_FLOAT)
        {
            rb_raise(rb_eTypeError, "Do not know how to convert attribute");
        }

        if (all_datatype == HYPERDATATYPE_GARBAGE)
        {
            all_datatype = elem_datatype;
        }

        if (elem_datatype != all_datatype)
        {
            rb_raise(rb_eTypeError, "Cannot store heterogeneous lists");
        }

        rb_str_append(*backing, elem_backing);
std::cerr << __FILE__ << ":" << __LINE__ << " TYPES " << elem_datatype << " " << all_datatype << std::endl;
    }

    if (all_datatype == HYPERDATATYPE_GARBAGE)
    {
        *datatype = HYPERDATATYPE_LIST_GENERIC;
    }
    else if (all_datatype == HYPERDATATYPE_STRING)
    {
        *datatype = HYPERDATATYPE_LIST_STRING;
    }
    else if (all_datatype == HYPERDATATYPE_INT64)
    {
        *datatype = HYPERDATATYPE_LIST_INT64;
    }
    else if (all_datatype == HYPERDATATYPE_FLOAT)
    {
        *datatype = HYPERDATATYPE_LIST_FLOAT;
    }
    else
    {
        abort();
    }
}

static bool
_compare_less(VALUE lhs, VALUE rhs)
{
    if (TYPE(lhs) != TYPE(rhs))
    {
        return TYPE(lhs) < TYPE(rhs);
    }

    const char* tmp;
    long tmp_sz;
    e::slice lhs_slice;
    e::slice rhs_slice;
    int64_t lhs_int;
    int64_t rhs_int;
    double lhs_float;
    double rhs_float;
    char lhs_int_buf[sizeof(int64_t)];
    char rhs_int_buf[sizeof(int64_t)];
    char lhs_float_buf[sizeof(double)];
    char rhs_float_buf[sizeof(double)];

    switch (TYPE(lhs))
    {
        case T_SYMBOL:
            tmp = rb_id2name(SYM2ID(lhs));
            lhs_slice = e::slice(tmp, strlen(tmp));
            tmp = rb_id2name(SYM2ID(rhs));
            rhs_slice = e::slice(tmp, strlen(tmp));
            return compare_string(lhs_slice, rhs_slice);
        case T_STRING:
            tmp = rb_str2cstr(lhs, &tmp_sz);
            lhs_slice = e::slice(tmp, tmp_sz);
            tmp = rb_str2cstr(rhs, &tmp_sz);
            rhs_slice = e::slice(tmp, tmp_sz);
            return compare_string(lhs_slice, rhs_slice);
        case T_FIXNUM:
        case T_BIGNUM:
            lhs_int = NUM2LL(lhs);
            e::pack64le(lhs_int, lhs_int_buf);
            lhs_slice = e::slice(lhs_int_buf, sizeof(int64_t));
            rhs_int = NUM2LL(rhs);
            e::pack64le(rhs_int, rhs_int_buf);
            rhs_slice = e::slice(rhs_int_buf, sizeof(int64_t));
            return compare_int64(lhs_slice, rhs_slice);
        case T_FLOAT:
            lhs_float = NUM2DBL(lhs);
            e::packdoublele(lhs_float, lhs_float_buf);
            lhs_slice = e::slice(lhs_float_buf, sizeof(double));
            rhs_float = NUM2DBL(rhs);
            e::packdoublele(rhs_float, rhs_float_buf);
            rhs_slice = e::slice(rhs_float_buf, sizeof(double));
            return compare_float(lhs_slice, rhs_slice);
        default:
            return false;
    }
}

static bool
_compare_key_less(VALUE lhs, VALUE rhs)
{
    VALUE lhs_key = Qnil;
    VALUE lhs_val = Qnil;
    VALUE rhs_key = Qnil;
    VALUE rhs_val = Qnil;
    lhs_key = rb_ary_entry(lhs, 0);
    lhs_val = rb_ary_entry(lhs, 1);
    rhs_key = rb_ary_entry(rhs, 0);
    rhs_val = rb_ary_entry(rhs, 1);
    return _compare_less(lhs_key, rhs_key);
}

static void
_hash_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    VALUE hash_pairs = Qnil;
    VALUE pair = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    VALUE key_backing = Qnil;
    VALUE val_backing = Qnil;
    VALUE* ary = NULL;
    enum hyperdatatype key_datatype = HYPERDATATYPE_GARBAGE;
    enum hyperdatatype val_datatype = HYPERDATATYPE_GARBAGE;
    enum hyperdatatype all_key_datatype = HYPERDATATYPE_GARBAGE;
    enum hyperdatatype all_val_datatype = HYPERDATATYPE_GARBAGE;
    *backing = rb_str_new("", 0);

    hash_pairs = rb_funcall(obj, rb_intern("to_a"), 0);
    // Sort hash_pairs
    ary = RARRAY(hash_pairs)->ptr;
    std::sort(ary, ary + RARRAY_LEN(hash_pairs), _compare_key_less);

    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        pair = rb_ary_entry(hash_pairs, i);
        key = rb_ary_entry(pair, 0);
        val = rb_ary_entry(pair, 1);
        hyperclient_elem_to_backing(key, &key_datatype, &key_backing);
        hyperclient_elem_to_backing(val, &val_datatype, &val_backing);

        if (key_backing == Qnil || val_backing == Qnil)
        {
            rb_raise(rb_eTypeError, "Do not know how to convert attribute");
        }

        if ((key_datatype != HYPERDATATYPE_STRING &&
             key_datatype != HYPERDATATYPE_INT64 &&
             key_datatype != HYPERDATATYPE_FLOAT) ||
            (val_datatype != HYPERDATATYPE_STRING &&
             val_datatype != HYPERDATATYPE_INT64 &&
             val_datatype != HYPERDATATYPE_FLOAT))
        {
            rb_raise(rb_eTypeError, "Do not know how to convert attribute");
        }

        if (all_key_datatype == HYPERDATATYPE_GARBAGE)
        {
            assert(all_val_datatype == HYPERDATATYPE_GARBAGE);
            all_key_datatype = key_datatype;
            all_val_datatype = val_datatype;
        }

        if (key_datatype != all_key_datatype ||
            val_datatype != all_val_datatype)
        {
            rb_raise(rb_eTypeError, "Cannot store heterogeneous maps");
        }

        rb_str_append(*backing, key_backing);
        rb_str_append(*backing, val_backing);
    }

    if (all_key_datatype == HYPERDATATYPE_GARBAGE)
    {
        *datatype = HYPERDATATYPE_MAP_GENERIC;
    }
    else if (all_key_datatype == HYPERDATATYPE_STRING)
    {
        if (all_val_datatype == HYPERDATATYPE_STRING)
        {
            *datatype = HYPERDATATYPE_MAP_STRING_STRING;
        }
        else if (all_val_datatype == HYPERDATATYPE_INT64)
        {
            *datatype = HYPERDATATYPE_MAP_STRING_INT64;
        }
        else if (all_val_datatype == HYPERDATATYPE_FLOAT)
        {
            *datatype = HYPERDATATYPE_MAP_STRING_FLOAT;
        }
        else
        {
            abort();
        }
    }
    else if (all_key_datatype == HYPERDATATYPE_INT64)
    {
        if (all_val_datatype == HYPERDATATYPE_STRING)
        {
            *datatype = HYPERDATATYPE_MAP_INT64_STRING;
        }
        else if (all_val_datatype == HYPERDATATYPE_INT64)
        {
            *datatype = HYPERDATATYPE_MAP_INT64_INT64;
        }
        else if (all_val_datatype == HYPERDATATYPE_FLOAT)
        {
            *datatype = HYPERDATATYPE_MAP_INT64_FLOAT;
        }
        else
        {
            abort();
        }
    }
    else if (all_key_datatype == HYPERDATATYPE_FLOAT)
    {
        if (all_val_datatype == HYPERDATATYPE_STRING)
        {
            *datatype = HYPERDATATYPE_MAP_FLOAT_STRING;
        }
        else if (all_val_datatype == HYPERDATATYPE_INT64)
        {
            *datatype = HYPERDATATYPE_MAP_FLOAT_INT64;
        }
        else if (all_val_datatype == HYPERDATATYPE_FLOAT)
        {
            *datatype = HYPERDATATYPE_MAP_FLOAT_FLOAT;
        }
        else
        {
            abort();
        }
    }
    else
    {
        abort();
    }
}

extern "C"
{

void
hyperclient_elem_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    switch (TYPE(obj))
    {
        case T_SYMBOL:
            _symbol_to_inner_backing(obj, datatype, backing);
            break;
        case T_STRING:
            _string_to_inner_backing(obj, datatype, backing);
            break;
        case T_FIXNUM:
        case T_BIGNUM:
            _num_to_backing(obj, datatype, backing);
            break;
        case T_FLOAT:
            _float_to_backing(obj, datatype, backing);
            break;
        default:
            rb_raise(rb_eTypeError, "Cannot convert object to a HyperDex type");
            break;
    }
}

void
hyperclient_ruby_obj_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    switch (TYPE(obj))
    {
        case T_SYMBOL:
            _symbol_to_backing(obj, datatype, backing);
            break;
        case T_STRING:
            _string_to_backing(obj, datatype, backing);
            break;
        case T_FIXNUM:
        case T_BIGNUM:
            _num_to_backing(obj, datatype, backing);
            break;
        case T_FLOAT:
            _float_to_backing(obj, datatype, backing);
            break;
        case T_ARRAY:
            _list_to_backing(obj, datatype, backing);
            break;
        case T_HASH:
            _hash_to_backing(obj, datatype, backing);
            break;
        default:
            rb_raise(rb_eTypeError, "Cannot convert object to a HyperDex type");
            break;
    }
}

extern "C"
{

void
hyperclient_ruby_free_attrs(hyperclient_attribute* attrs)
{
    delete[] attrs;
}

void
hyperclient_ruby_free_map_attrs(hyperclient_map_attribute* attrs)
{
    delete[] attrs;
}

}

void
hyperclient_ruby_hash_to_attrs(VALUE hash, VALUE* backing,
                               struct hyperclient_attribute** attrs,
                               size_t* attrs_sz)
{
    VALUE hash_pairs = Qnil;
    VALUE pair = Qnil;
    VALUE attr_name = Qnil;
    VALUE value = Qnil;
    VALUE value_backing = Qnil;
    enum hyperdatatype value_type = HYPERDATATYPE_GARBAGE;
    long int value_sz = 0;

    // Allocate the attrs
    *backing = rb_ary_new();
    *attrs_sz = FIX2UINT(rb_funcall(hash, rb_intern("size"), 0));
    *attrs = new (std::nothrow) hyperclient_attribute[*attrs_sz];

    if (*attrs == NULL)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return;
    }

    value_backing = Data_Wrap_Struct(rb_cData, NULL, hyperclient_ruby_free_attrs, *attrs);
    rb_ary_push(*backing, value_backing);
    hash_pairs = rb_funcall(hash, rb_intern("to_a"), 0);

    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        pair = rb_ary_entry(hash_pairs, i);
        attr_name = rb_ary_entry(pair, 0);
        value = rb_ary_entry(pair, 1);

        hyperclient_ruby_obj_to_backing(value, &value_type, &value_backing);

        if (value_backing == Qnil)
        {
            rb_raise(rb_eTypeError, "Do not know how to convert attribute");
        }

        rb_ary_push(*backing, value_backing);
        (*attrs)[i].attr     = StringValueCStr(attr_name);
        (*attrs)[i].value    = rb_str2cstr(value_backing, &value_sz);
        (*attrs)[i].value_sz = value_sz;
        (*attrs)[i].datatype = value_type;
    }
}

void
hyperclient_ruby_hash_to_map_attrs(VALUE hash, VALUE* backings,
                                   struct hyperclient_map_attribute** attrs,
                                   size_t* attrs_sz)
{
    VALUE hash_pairs = Qnil;
    //VALUE kbacking = Qnil;
    //VALUE vbacking = Qnil;
    VALUE pair = Qnil;
    VALUE name = Qnil;
    VALUE inner_hash_pairs = Qnil;
    size_t idx = 0;

    *backings = rb_ary_new();
    *attrs = NULL;
    *attrs_sz = 0;
    hash_pairs = rb_funcall(hash, rb_intern("to_a"), 0);

    // Compute the number of hyperclient_map_attribute structs needed
    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        pair = rb_ary_entry(hash_pairs, i);
        *attrs_sz += FIX2UINT(rb_funcall(hash, rb_intern("size"), 0));
    }

    // Allocate it
    *attrs = new (std::nothrow) hyperclient_map_attribute[*attrs_sz];

    if (*attrs == NULL)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return;
    }

    rb_ary_push(*backings, Data_Wrap_Struct(rb_cData, NULL, hyperclient_ruby_free_map_attrs, *attrs));

    // Now pack!
    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        pair = rb_ary_entry(hash_pairs, i);
        name = rb_ary_entry(pair, 0);
        inner_hash_pairs = rb_funcall(rb_ary_entry(pair, 1), rb_intern("to_a"), 0);

        for (ssize_t j = 0; j < RARRAY_LEN(inner_hash_pairs); ++j)
        {
        }
    }

#if 0
 {
        pair = rb_ary_entry(hash_pairs, i);
        attr_name = rb_ary_entry(pair, 0);
        value = rb_ary_entry(pair, 1);

        hyperclient_ruby_obj_to_backing(value, &value_type, &value_backing);

        if (value_backing == Qnil)
        {
            rb_raise(rb_eTypeError, "Do not know how to convert attribute");
        }

        rb_ary_push(*backing, value_backing);
        (*attrs)[i].attr     = StringValueCStr(attr_name);
        (*attrs)[i].value    = rb_str2cstr(value_backing, &value_sz);
        (*attrs)[i].value_sz = value_sz;
        (*attrs)[i].datatype = value_type;
    }   }



    for a in value:
        name, b = a
        keytype = None
        valtype = None
        j = i
        if isinstance(b, dict):
            for k, v in b.iteritems():
                kdatatype, kbacking = _obj_to_backing(k)
                vdatatype, vbacking = _obj_to_backing(v)
                if kdatatype not in (keytype, None):
                    mixedtype = TypeError("Cannot store heterogeneous maps")
                keytype = kdatatype
                if vdatatype not in (valtype, None):
                    mixedtype = TypeError("Cannot store heterogeneous maps")
                valtype = vdatatype
                backings.append(kbacking)
                backings.append(vbacking)
                attrs[0][i].attr = name
                attrs[0][i].map_key = kbacking
                attrs[0][i].map_key_sz = len(kbacking)
                attrs[0][i].map_key_datatype = kdatatype
                attrs[0][i].value = vbacking
                attrs[0][i].value_sz = len(vbacking)
                attrs[0][i].value_datatype = vdatatype
                i += 1
        else:
            kdatatype, kbacking = _obj_to_backing(b)
            attrs[0][i].attr = name
            attrs[0][i].map_key = kbacking
            attrs[0][i].map_key_sz = len(kbacking)
            attrs[0][i].map_key_datatype = kdatatype
            attrs[0][i].value = NULL
            attrs[0][i].value_sz = 0
            attrs[0][i].value_datatype = HYPERDATATYPE_GENERIC;
            i += 1
    return backings
#endif
}

}

static VALUE
_attr_to_string(struct hyperclient_attribute* attr)
{
    return rb_str_new(attr->value, attr->value_sz);
}

static VALUE
_attr_to_int64(struct hyperclient_attribute* attr)
{
    int64_t ret = 0;
    char buf[sizeof(int64_t)];

    if (attr->value_sz >= sizeof(int64_t))
    {
        e::unpack64le(attr->value, &ret);
    }
    else
    {
        memset(buf, 0, sizeof(int64_t));
        memmove(buf, attr->value, attr->value_sz);
        e::unpack64le(buf, &ret);
    }

    return LL2NUM(ret);
}

static VALUE
_attr_to_float(struct hyperclient_attribute* attr)
{
    double ret = 0;
    char buf[sizeof(double)];

    if (attr->value_sz >= sizeof(double))
    {
        e::unpackdoublele(attr->value, &ret);
    }
    else
    {
        memset(buf, 0, sizeof(double));
        memmove(buf, attr->value, attr->value_sz);
        e::unpackdoublele(buf, &ret);
    }

    return rb_float_new(ret);
}

static VALUE
_attr_to_list_string(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE lst = Qnil;
    VALUE str = Qnil;
    uint32_t sz;

    lst = rb_ary_new();

    while (rem >= sizeof(uint32_t))
    {
        e::unpack32le(attr->value + pos, &sz);

        if (rem - sizeof(uint32_t) < sz)
        {
            rb_raise(rb_eRuntimeError, "list(string) is improperly structured (file a bug)");
        }

        str = rb_str_new(attr->value + pos + sizeof(uint32_t), sz);
        pos += sizeof(uint32_t) + sz;
        rem -= sizeof(uint32_t) + sz;
        rb_ary_push(lst, str);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "list(string) is improperly structured (file a bug)");
    }

    return lst;
}

static VALUE
_attr_to_list_int64(struct hyperclient_attribute* attr)
{
    VALUE lst = Qnil;
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    int64_t elem;

    lst = rb_ary_new();

    while (pos + sizeof(int64_t) <= end)
    {
        pos = e::unpack64le(pos, &elem);
        rb_ary_push(lst, LL2NUM(elem));
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "list(int64) contains excess data (file a bug)");
    }

    return lst;
}

static VALUE
_attr_to_list_float(struct hyperclient_attribute* attr)
{
    VALUE lst = Qnil;
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    double elem;

    lst = rb_ary_new();

    while (pos + sizeof(double) <= end)
    {
        pos = e::unpackdoublele(pos, &elem);
        rb_ary_push(lst, rb_float_new(elem));
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "list(float) contains excess data (file a bug)");
    }

    return lst;
}

static VALUE
_attr_to_set_string(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE set = Qnil;
    VALUE str = Qnil;
    uint32_t sz;

    set = rb_class_new_instance(0, NULL, rb_path2class("Set"));

    while (rem >= sizeof(uint32_t))
    {
        e::unpack32le(attr->value + pos, &sz);

        if (rem - sizeof(uint32_t) < sz)
        {
            rb_raise(rb_eRuntimeError, "set(string) is improperly structured (file a bug)");
        }

        str = rb_str_new(attr->value + pos + sizeof(uint32_t), sz);
        pos += sizeof(uint32_t) + sz;
        rem -= sizeof(uint32_t) + sz;
        rb_funcall(set, rb_intern("add"), 1, str);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "set(string) is improperly structured (file a bug)");
    }

    return set;
}

static VALUE
_attr_to_set_int64(struct hyperclient_attribute* attr)
{
    VALUE set = Qnil;
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    int64_t elem;

    set = rb_class_new_instance(0, NULL, rb_path2class("Set"));

    while (pos + sizeof(int64_t) <= end)
    {
        pos = e::unpack64le(pos, &elem);
        rb_funcall(set, rb_intern("add"), 1, LL2NUM(elem));
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "set(int64) contains excess data (file a bug)");
    }

    return set;
}

static VALUE
_attr_to_set_float(struct hyperclient_attribute* attr)
{
    VALUE set = Qnil;
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    double elem;

    set = rb_class_new_instance(0, NULL, rb_path2class("Set"));

    while (pos + sizeof(double) <= end)
    {
        pos = e::unpackdoublele(pos, &elem);
        rb_funcall(set, rb_intern("add"), 1, rb_float_new(elem));
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "set(float) contains excess data (file a bug)");
    }

    return set;
}

static VALUE
_attr_to_map_string_string(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    uint32_t key_sz;
    uint32_t val_sz;

    map = rb_hash_new();

    while (rem >= 2 * sizeof(uint32_t))
    {
        e::unpack32le(attr->value + pos, &key_sz);

        if (rem < 2 * sizeof(uint32_t) + key_sz)
        {
            rb_raise(rb_eRuntimeError, "map(string,string) is improperly structured (file a bug)");
        }

        key = rb_str_new(attr->value + pos + sizeof(uint32_t), key_sz);
        e::unpack32le(attr->value + pos + sizeof(uint32_t) + key_sz, &val_sz);

        if (rem < 2 * sizeof(uint32_t) + key_sz + val_sz)
        {
            rb_raise(rb_eRuntimeError, "map(string,string) is improperly structured (file a bug)");
        }

        val = rb_str_new(attr->value + pos + sizeof(uint32_t) + key_sz + sizeof(uint32_t), val_sz);
        rem -= 2 * sizeof(uint32_t) + key_sz + val_sz;
        pos += 2 * sizeof(uint32_t) + key_sz + val_sz;
        rb_hash_aset(map, key, val);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "map(string,string) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_string_int64(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    uint32_t key_sz;
    int64_t val_i;

    map = rb_hash_new();

    while (rem >= sizeof(uint32_t) + sizeof(int64_t))
    {
        e::unpack32le(attr->value + pos, &key_sz);

        if (rem < sizeof(uint32_t) + key_sz + sizeof(int64_t))
        {
            rb_raise(rb_eRuntimeError, "map(string,int64) is improperly structured (file a bug)");
        }

        key = rb_str_new(attr->value + pos + sizeof(uint32_t), key_sz);
        e::unpack64le(attr->value + pos + sizeof(uint32_t) + key_sz, &val_i);
        val = LL2NUM(val_i);
        rem -= sizeof(uint32_t) + key_sz + sizeof(int64_t);
        pos += sizeof(uint32_t) + key_sz + sizeof(int64_t);
        rb_hash_aset(map, key, val);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "map(string,int64) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_string_float(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    uint32_t key_sz;
    double val_d;

    map = rb_hash_new();

    while (rem >= sizeof(uint32_t) + sizeof(double))
    {
        e::unpack32le(attr->value + pos, &key_sz);

        if (rem < sizeof(uint32_t) + key_sz + sizeof(double))
        {
            rb_raise(rb_eRuntimeError, "map(string,float) is improperly structured (file a bug)");
        }

        key = rb_str_new(attr->value + pos + sizeof(uint32_t), key_sz);
        e::unpackdoublele(attr->value + pos + sizeof(uint32_t) + key_sz, &val_d);
        val = rb_float_new(val_d);
        rem -= sizeof(uint32_t) + key_sz + sizeof(double);
        pos += sizeof(uint32_t) + key_sz + sizeof(double);
        rb_hash_aset(map, key, val);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "map(string,float) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_int64_string(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    int64_t key_i;
    uint32_t val_sz;

    map = rb_hash_new();

    while (rem >= sizeof(int64_t) + sizeof(uint32_t))
    {
        e::unpack64le(attr->value + pos, &key_i);
        key = LL2NUM(key_i);
        e::unpack32le(attr->value + pos + sizeof(int64_t), &val_sz);

        if (rem < sizeof(int64_t) + sizeof(uint32_t) + val_sz)
        {
            rb_raise(rb_eRuntimeError, "map(int64,string) is improperly structured (file a bug)");
        }

        val = rb_str_new(attr->value + pos + sizeof(int64_t) + sizeof(uint32_t), val_sz);
        rem -= sizeof(int64_t) + sizeof(uint32_t) + val_sz;
        pos += sizeof(int64_t) + sizeof(uint32_t) + val_sz;
        rb_hash_aset(map, key, val);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "map(int64,string) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_int64_int64(struct hyperclient_attribute* attr)
{
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    int64_t key_i;
    int64_t val_i;

    map = rb_hash_new();

    while (pos + sizeof(int64_t) + sizeof(int64_t) <= end)
    {
        pos = e::unpack64le(pos, &key_i);
        pos = e::unpack64le(pos, &val_i);
        key = LL2NUM(key_i);
        val = LL2NUM(val_i);
        rb_hash_aset(map, key, val);
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "map(int64,int64) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_int64_float(struct hyperclient_attribute* attr)
{
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    int64_t key_i;
    double val_d;

    map = rb_hash_new();

    while (pos + sizeof(int64_t) + sizeof(double) <= end)
    {
        pos = e::unpack64le(pos, &key_i);
        pos = e::unpackdoublele(pos, &val_d);
        key = LL2NUM(key_i);
        val = rb_float_new(val_d);
        rb_hash_aset(map, key, val);
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "map(int64,float) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_float_string(struct hyperclient_attribute* attr)
{
    size_t pos = 0;
    size_t rem = attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    double key_d;
    uint32_t val_sz;

    map = rb_hash_new();

    while (rem >= sizeof(double) + sizeof(uint32_t))
    {
        e::unpackdoublele(attr->value + pos, &key_d);
        key = rb_float_new(key_d);
        e::unpack32le(attr->value + pos + sizeof(double), &val_sz);

        if (rem < sizeof(double) + sizeof(uint32_t) + val_sz)
        {
            rb_raise(rb_eRuntimeError, "map(int64,string) is improperly structured (file a bug)");
        }

        val = rb_str_new(attr->value + pos + sizeof(double) + sizeof(uint32_t), val_sz);
        rem -= sizeof(double) + sizeof(uint32_t) + val_sz;
        pos += sizeof(double) + sizeof(uint32_t) + val_sz;
        rb_hash_aset(map, key, val);
    }

    if (rem > 0)
    {
        rb_raise(rb_eRuntimeError, "map(int64,string) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_float_int64(struct hyperclient_attribute* attr)
{
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    double key_d;
    int64_t val_i;

    map = rb_hash_new();

    while (pos + sizeof(double) + sizeof(int64_t) <= end)
    {
        pos = e::unpackdoublele(pos, &key_d);
        pos = e::unpack64le(pos, &val_i);
        key = rb_float_new(key_d);
        val = LL2NUM(val_i);
        rb_hash_aset(map, key, val);
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "map(float,int64) contains excess data (file a bug)");
    }

    return map;
}

static VALUE
_attr_to_map_float_float(struct hyperclient_attribute* attr)
{
    const char* pos = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE map = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    double key_d;
    double val_d;

    map = rb_hash_new();

    while (pos + sizeof(double) + sizeof(double) <= end)
    {
        pos = e::unpackdoublele(pos, &key_d);
        pos = e::unpackdoublele(pos, &val_d);
        key = rb_float_new(key_d);
        val = rb_float_new(val_d);
        rb_hash_aset(map, key, val);
    }

    if (pos != end)
    {
        rb_raise(rb_eRuntimeError, "map(float,float) contains excess data (file a bug)");
    }

    return map;
}

extern "C"
{

VALUE
hyperclient_ruby_attrs_to_hash(struct hyperclient_attribute* attrs, size_t attrs_sz)
{
    VALUE ret = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;

    ret = rb_hash_new();

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        key = rb_str_new2(attrs[i].attr);

        switch (attrs[i].datatype)
        {
            case HYPERDATATYPE_STRING:
                val = _attr_to_string(attrs + i);
                break;
            case HYPERDATATYPE_INT64:
                val = _attr_to_int64(attrs + i);
                break;
            case HYPERDATATYPE_FLOAT:
                val = _attr_to_float(attrs + i);
                break;
            case HYPERDATATYPE_LIST_STRING:
                val = _attr_to_list_string(attrs + i);
                break;
            case HYPERDATATYPE_LIST_INT64:
                val = _attr_to_list_int64(attrs + i);
                break;
            case HYPERDATATYPE_LIST_FLOAT:
                val = _attr_to_list_float(attrs + i);
                break;
            case HYPERDATATYPE_SET_STRING:
                val = _attr_to_set_string(attrs + i);
                break;
            case HYPERDATATYPE_SET_INT64:
                val = _attr_to_set_int64(attrs + i);
                break;
            case HYPERDATATYPE_SET_FLOAT:
                val = _attr_to_set_float(attrs + i);
                break;
            case HYPERDATATYPE_MAP_STRING_STRING:
                val = _attr_to_map_string_string(attrs + i);
                break;
            case HYPERDATATYPE_MAP_STRING_INT64:
                val = _attr_to_map_string_int64(attrs + i);
                break;
            case HYPERDATATYPE_MAP_STRING_FLOAT:
                val = _attr_to_map_string_float(attrs + i);
                break;
            case HYPERDATATYPE_MAP_INT64_STRING:
                val = _attr_to_map_int64_string(attrs + i);
                break;
            case HYPERDATATYPE_MAP_INT64_INT64:
                val = _attr_to_map_int64_int64(attrs + i);
                break;
            case HYPERDATATYPE_MAP_INT64_FLOAT:
                val = _attr_to_map_int64_float(attrs + i);
                break;
            case HYPERDATATYPE_MAP_FLOAT_STRING:
                val = _attr_to_map_float_string(attrs + i);
                break;
            case HYPERDATATYPE_MAP_FLOAT_INT64:
                val = _attr_to_map_float_int64(attrs + i);
                break;
            case HYPERDATATYPE_MAP_FLOAT_FLOAT:
                val = _attr_to_map_float_float(attrs + i);
                break;
            case HYPERDATATYPE_GENERIC:
            case HYPERDATATYPE_LIST_GENERIC:
            case HYPERDATATYPE_SET_GENERIC:
            case HYPERDATATYPE_MAP_GENERIC:
            case HYPERDATATYPE_MAP_STRING_KEYONLY:
            case HYPERDATATYPE_MAP_INT64_KEYONLY:
            case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
            case HYPERDATATYPE_GARBAGE:
            default:
                rb_raise(rb_eRuntimeError, "trying to deserialize invalid type (file a bug)");
        }

        rb_hash_aset(ret, key, val);
    }

    return ret;
}

}
