%typemap(javacode) HyperClient
%{
  java.util.HashMap<Long,Deferred> ops = new java.util.HashMap<Long,Deferred>(); 

  void loop() throws HyperClientException
  {
    SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

    long ret = loop(rc_int_ptr);

    ReturnCode rc = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));

    if ( ret < 0 )
    {
      throw new HyperClientException(rc);
    }
    else
    {
      Deferred d = ops.get(ret);
      d.status = rc;
      d.callback();
    }
  }

  // Deal's with Java's int size limit for objects.
  // Return the int closest to size_t representation
  // Ideally, in a future java version, this method will
  // return the same value ie., do nothing or simply be removed.
  //
  static int size_t_to_int(long l)
  {
    int i = 0;

    if ( l < 0 || l > Integer.MAX_VALUE )
    {
        i = Integer.MAX_VALUE;
    }
    else
    {
        i = (int)l;
    }

    return i;
  }

  static java.util.Map attrs_to_dict(hyperclient_attribute attrs, long attrs_sz)
                                                                throws ValueError
  {
    java.util.HashMap<Object,Object> map = new java.util.HashMap<Object,Object>();

    int sz = HyperClient.size_t_to_int(attrs_sz);

    for ( int i=0; i<sz; i++)
    {
        hyperclient_attribute ha = get_attr(attrs,i);
        map.put(ha.getAttrName(),ha.getAttrValue());
    }

    return map;
  }

  static hyperclient_attribute dict_to_attrs(java.util.Map attrsMap) throws TypeError,
                                                                        MemoryError
  {
    int attrs_sz = attrsMap.size();
    hyperclient_attribute attrs = alloc_attrs(attrs_sz);

    if ( attrs == null ) throw new MemoryError();
    
    int i = 0;

    for (java.util.Iterator it=attrsMap.keySet().iterator(); it.hasNext();)
    {
        hyperclient_attribute ha = get_attr(attrs,i);

        try
        {
            String attrStr = (String)(it.next());
            if ( attrStr == null ) throw new TypeError("Attribute name cannot be null");

            byte[] attrBytes = attrStr.getBytes();

            Object value = attrsMap.get(attrStr);

        
            if ( value == null ) throw new TypeError(
                                        "Cannot convert null value "
                                    + "for attribute '" + attrStr + "'");

            hyperdatatype type = null;

            if ( value instanceof String )
            {
                type = hyperdatatype.HYPERDATATYPE_STRING;
                byte[] valueBytes = ((String)value).getBytes();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
    
            }
            else if ( value instanceof Long )
            {
                type = hyperdatatype.HYPERDATATYPE_INT64;
                byte[] valueBytes = java.nio.ByteBuffer.allocate(8).order(
                    java.nio.ByteOrder.LITTLE_ENDIAN).putLong(
                        ((Long)value).longValue()).array();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
            }
            else if ( value instanceof java.util.List )
            {
                java.util.List list = (java.util.List)value;

                type = hyperdatatype.HYPERDATATYPE_LIST_GENERIC;

                for (java.util.Iterator l_it=list.iterator();l_it.hasNext();)
                {
                    Object val = l_it.next();

                    if ( val == null ) throw new TypeError(
                                        "Cannot convert null element "
                                    + "for list attribute '" + attrStr + "'");

                    if (type == hyperdatatype.HYPERDATATYPE_LIST_GENERIC)
                    {
                        if (val instanceof String)
                            type = hyperdatatype.HYPERDATATYPE_LIST_STRING;
                        else if (val instanceof Long)
                            type = hyperdatatype.HYPERDATATYPE_LIST_INT64;
                        else
                            throw new TypeError(
                                "Do not know how to convert type '"
                                    + val.getClass().getName()
                                    + "' for list attribute '" + attrStr + "'");
                    }
                    else
                    {
                        if ( (val instanceof String
                                && type != hyperdatatype.HYPERDATATYPE_LIST_STRING)
                            || (val instanceof Long
                                && type != hyperdatatype.HYPERDATATYPE_LIST_INT64) )

                            throw new TypeError("Cannot store heterogeneous lists");
                    }

                    if ( type == hyperdatatype.HYPERDATATYPE_LIST_STRING )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)val).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)val).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_LIST_INT64 )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)val).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                }
            }
            else if ( value instanceof java.util.Set )
            {
                // XXX HyderDex seems to quietly fail if set is not sorted
                // So I make sure a TreeSet ie., sorted set is used.
                // I was wondering why the python binding went through 
                // the trouble of sorting a set right before packing it.

                java.util.Set<?> set = (java.util.Set<?>)value;

                if ( ! (set instanceof java.util.SortedSet) )
                {
                    set = new java.util.TreeSet<Object>(set);
                }

                type = hyperdatatype.HYPERDATATYPE_SET_GENERIC;

                for (java.util.Iterator s_it=set.iterator();s_it.hasNext();)
                {
                    Object val = s_it.next();

                    if ( val == null ) throw new TypeError(
                                        "Cannot convert null element "
                                    + "for set attribute '" + attrStr + "'");

                    if (type == hyperdatatype.HYPERDATATYPE_SET_GENERIC)
                    {
                        if (val instanceof String)
                            type = hyperdatatype.HYPERDATATYPE_SET_STRING;
                        else if (val instanceof Long)
                            type = hyperdatatype.HYPERDATATYPE_SET_INT64;
                        else
                            throw new TypeError(
                                "Do not know how to convert type '"
                                    + val.getClass().getName()
                                    + "' for set attribute '" + attrStr + "'");
                    }
                    else
                    {
                        if ( (val instanceof String
                                && type != hyperdatatype.HYPERDATATYPE_SET_STRING)
                            || (val instanceof Long
                                && type != hyperdatatype.HYPERDATATYPE_SET_INT64) )

                            throw new TypeError("Cannot store heterogeneous sets");
                    }

                    if ( type == hyperdatatype.HYPERDATATYPE_SET_STRING )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)val).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)val).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
    
                    if ( type == hyperdatatype.HYPERDATATYPE_SET_INT64 )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)val).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                }
            }
            else if ( value instanceof java.util.Map )
            {
                java.util.Map<?,?> map = (java.util.Map<?,?>)value;

                if ( ! (map instanceof java.util.SortedMap) )
                {
                    map = new java.util.TreeMap<Object,Object>(map);
                }

                type = hyperdatatype.HYPERDATATYPE_MAP_GENERIC;

                for (java.util.Iterator m_it=map.keySet().iterator();m_it.hasNext();)
                {
                    Object key = m_it.next();

                    if ( key == null ) throw new TypeError(
                                      "In attribute '" + attrStr 
                                    + "': A non-empty map cannot have a null key entry"
                                    + "for set attribute '" + attrStr + "'");

                    Object val = map.get(key);

                    if ( val == null ) throw new TypeError(
                                      "In attribute '" + attrStr 
                                    + "': A non-empty map cannot have a null value entry"
                                    + "for set attribute '" + attrStr + "'");

                    // Initialize map type
                    //
                    if ( type ==  hyperdatatype.HYPERDATATYPE_MAP_GENERIC )
                    {
                        if ( key instanceof String && val instanceof String )
                        {
                            type = hyperdatatype.HYPERDATATYPE_MAP_STRING_STRING;
                        }    
                        else if ( key instanceof String && val instanceof Long )
                        {
                            type = hyperdatatype.HYPERDATATYPE_MAP_STRING_INT64;
                        }    
                        else if ( key instanceof Long && val instanceof String )
                        {
                            type = hyperdatatype.HYPERDATATYPE_MAP_INT64_STRING;
                        }    
                        else if ( key instanceof Long && val instanceof Long )
                        {
                            type = hyperdatatype.HYPERDATATYPE_MAP_INT64_INT64;
                        }    
                        else
                        {
                            throw new TypeError(
                                "Do not know how to convert map type '("
                                    + key.getClass().getName() + ","
                                    + val.getClass().getName() + ")"
                                    + "' for set attribute '" + attrStr + "'");
                        }
                    }
                    else // Make sure it is always this map type (not heterogenious)
                    {
                        if (  (key instanceof String && val instanceof String 
                                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_STRING)
                            ||
                              (key instanceof String && val instanceof Long 
                                 && type != hyperdatatype.HYPERDATATYPE_MAP_STRING_INT64)
                            ||
                              (key instanceof Long && val instanceof String 
                                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_STRING)
                            ||
                              (key instanceof Long && val instanceof Long 
                                 && type != hyperdatatype.HYPERDATATYPE_MAP_INT64_INT64)
                        )
                        {
                            throw new TypeError("Cannot store heterogeneous maps");
                        }
                    }

                    if ( key instanceof String )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)key).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)key).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
                    else
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)key).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }

                    if ( val instanceof String )
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(4).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN).putInt(
                                                 ((String)val).getBytes().length).array()) == 0)
                                                 throw new MemoryError();
    
                        if (write_attr_value(ha, ((String)val).getBytes()) == 0)
                                                 throw new MemoryError();
                    }
                    else
                    {
                        if (write_attr_value(ha, java.nio.ByteBuffer.allocate(8).order( 
                                                 java.nio.ByteOrder.LITTLE_ENDIAN
                                                 ).putLong(((Long)val).longValue()
                                                 ).array()) == 0)
                                                 throw new MemoryError();
                    }
                }
            }
            else
            {
                throw new TypeError(
                     "Do not know how to convert type '"
                        + value.getClass().getName()
                        + "' for attribute '" + attrStr + "'");
            }

            if (write_attr_name(ha, attrBytes, type) == 0) throw new MemoryError();
        }
        catch(MemoryError me)
        {
            destroy_attrs(attrs, attrs_sz);
            throw me;
        }
        catch(TypeError te)
        {
            destroy_attrs(attrs, attrs_sz);
            throw te;
        }

        i++;
    }

    return attrs;
  }

  public Deferred async_get(String space, String key) throws HyperClientException,
                                                             ValueError
  {
    return new DeferredGet(this,space, key);
  }

  public java.util.Map get(String space, String key) throws HyperClientException,
                                                            ValueError
  {
    DeferredGet d = (DeferredGet)(async_get(space, key));
    return (java.util.Map)(d.waitFor());
  }

  public Deferred async_put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    return new DeferredFromAttrs(this, new SimpleOpPut(this), space, key, map);
  }

  public boolean put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError,
                                                            ValueError
  {
    Deferred d = (DeferredFromAttrs)(async_put(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }

  public Deferred async_delete(String space, String key) throws HyperClientException
  {
    return new DeferredDelete(this, space, key);
  }

  public boolean delete(String space, String key) throws HyperClientException,
                                                         ValueError
  {
    Deferred d = (DeferredDelete)(async_delete(space, key));
    return ((Boolean)(d.waitFor())).booleanValue();
  }
%}
