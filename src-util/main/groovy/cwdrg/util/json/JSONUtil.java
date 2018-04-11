package cwdrg.util.json;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * in case we use simple JSON serialization for some structures for easy persist/retrieve from the database
 * 
 * @author a999166
 */

public final class JSONUtil
{
  private static final Logger       log          = LoggerFactory.getLogger(JSONUtil.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final ObjectMapper prettyMapper = getPrettyMapper();


  static ObjectMapper getPrettyMapper()
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    return mapper;
  }


  public static String serialize(Object o)
  {
    try {
      return objectMapper.writeValueAsString(o);
    } catch (JsonProcessingException jpe) {
      throw new RuntimeException("JSON serialization mapping error", jpe);
    }
  }


  public static void serialize(Object o, Writer writer)
  {
    try {
      objectMapper.writeValue(writer, o);
    } catch (JsonProcessingException jpe) {
      throw new RuntimeException("JSON serialization mapping error", jpe);
    } catch (IOException ioe) {
      throw new RuntimeException("I/O error", ioe);
    }

  }


  public static String logJSON(Object o)
  {
    try {
      return serialize(o);
    } catch (Exception e) {
      log.trace("log serialization error", e);
      return "JSON ERROR";
    }
  }


  public static Object deserialize(String json, Class<?> clazz)
  {
    if (StringUtils.isBlank(json)) { return null; }
    try {
      return objectMapper.readValue(json, clazz);
    } catch (JsonParseException jpe) {
      throw new RuntimeException("JSON deserialization parse error on (type: " + (clazz == null ? null : clazz.toString()) + "):: " + json, jpe);
    } catch (JsonMappingException jme) {
      throw new RuntimeException("JSON deserialization mapping error on (type: " + (clazz == null ? null : clazz.toString()) + "):: " + json, jme);
    } catch (IOException ioe) {
      throw new RuntimeException("JSON deserialization I/O error on (type: " + (clazz == null ? null : clazz.toString()) + "):: " + json, ioe);
    }
  }


  public static Object deserialize(String json, TypeReference<?> typeref)
  {
    if (StringUtils.isBlank(json)) { return null; }
    try {
      return objectMapper.readValue(json, typeref);
    } catch (JsonParseException jpe) {
      throw new RuntimeException("JSON deserialization parse error on (type: " + (typeref == null ? null : typeref.getType().toString()) + ") :: " + json, jpe);
    } catch (JsonMappingException jme) {
      throw new RuntimeException("JSON deserialization mapping error on (type: " + (typeref == null ? null : typeref.getType().toString()) + "):: " + json, jme);
    } catch (IOException ioe) {
      throw new RuntimeException("JSON deserialization I/O error on deserialize for (type: " + (typeref == null ? null : typeref.getType().toString()) + "):: " + json, ioe);
    }
  }


  public static List<String> deserializeList(String json)
  {
    if (StringUtils.isBlank(json)) { return null; }
    List<String> strs = (List<String>) JSONUtil.deserialize(json, new TypeReference<List<Object>>()
    {
    });
    return strs;
  }


  public static List<String> deserializeStringList(String json)
  {
    if (StringUtils.isBlank(json)) { return null; }
    List<String> strs = (List<String>) JSONUtil.deserialize(json, new TypeReference<List<String>>()
    {
    });
    return strs;
  }


  public static Set<String> deserializeStringSet(String json)
  {
    if (StringUtils.isBlank(json)) { return null; }
    Set<String> strs = (Set<String>) JSONUtil.deserialize(json, new TypeReference<Set<String>>()
    {
    });
    return strs;
  }


  public static Map<String, Object> deserializeMap(String json)
  {
    if (StringUtils.isBlank(json)) { return Collections.emptyMap(); }
    return (Map<String, Object>) deserialize(json, new TypeReference<Map<String, Object>>()
    {
    });
  }


  public static String toJSON(Object o)
  {
    try {
      return objectMapper.writeValueAsString(o);
    } catch (Exception e) {
      return null;
    }

  }


  public static String toJSONPretty(Object o)
  {
    try {
      return prettyMapper.writeValueAsString(o);
    } catch (Exception e) {
      return null;
    }
  }


  public static Map fromJSON(String json)
  {
    try {
      if (StringUtils.isBlank(json)) return new HashMap();
      return objectMapper.readValue(json, new TypeReference<Map<String, Object>>()
      {
      });
    } catch (Exception e) {
      return null;
    }
  }


  public static List fromJSONList(String json)
  {
    try {
      if (StringUtils.isBlank(json)) return new ArrayList();
      return objectMapper.readValue(json, new TypeReference<List<String>>()
      {
      });
    } catch (Exception e) {
      return null;
    }

  }


  public static List fromJSONArrayOfMap(String json)
  {
    try {
      if (StringUtils.isBlank(json)) return new ArrayList();
      return objectMapper.readValue(json, new TypeReference<List<Map<String, Object>>>()
      {
      });
    } catch (Exception e) {
      return null;
    }

  }


  private JSONUtil()
  {
    // hide constructor for utility class
  }

}
