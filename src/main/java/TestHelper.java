import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;

public class TestHelper {
    private static final Faker faker = new Faker();

    public static List<String> getNMockNames(int n) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            names.add(faker.name().firstName());
        }
        return names;
    }



}
