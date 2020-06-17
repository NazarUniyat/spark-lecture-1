package utils;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Field;

@UtilityClass
public class SchemaUtils {

    public StructType toSchema(Class clazz) {

        Field[] fields = clazz.getDeclaredFields();
        StructType result = new StructType();

        for (Field field : fields) {
            result = result.add(field.getName(), field.getType().getSimpleName());
        }

        return result;
    }
}
