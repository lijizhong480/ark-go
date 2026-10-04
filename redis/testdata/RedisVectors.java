// Reproduce the byte vectors with the Spring Data Redis and Jackson versions
// used by the Java service. This file is test data; it is not Go production code.
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

public class RedisVectors {
    static String hex(byte[] data) {
        StringBuilder out = new StringBuilder();
        for (byte b : data) out.append(String.format("%02x", b & 255));
        return out.toString();
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.activateDefaultTyping(mapper.getPolymorphicTypeValidator(),
                ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
        GenericJackson2JsonRedisSerializer json = new GenericJackson2JsonRedisSerializer(mapper);
        GenericJackson2JsonRedisSerializer plainJson = new GenericJackson2JsonRedisSerializer(new ObjectMapper());
        StringRedisSerializer string = new StringRedisSerializer();
        JdkSerializationRedisSerializer jdk = new JdkSerializationRedisSerializer();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("name", "Ark");
        value.put("count", 2);
        System.out.println("string=" + hex(string.serialize("héllo")));
        System.out.println("jsonString=" + hex(json.serialize("hello")));
        System.out.println("jsonMap=" + hex(json.serialize(value)));
        System.out.println("jsonMapPlain=" + hex(plainJson.serialize(value)));
        System.out.println("jdkString=" + hex(jdk.serialize("hello")));
    }
}
