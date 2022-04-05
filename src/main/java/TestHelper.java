import com.github.javafaker.Faker;

import java.util.*;

public class TestHelper {
    private static final Faker faker = new Faker(new Random(0));
    private static final Map<Integer, List<String>> firstNames = new HashMap<>();
    private static final Map<Integer, List<String>> lastNames = new HashMap<>();

    public static List<String> getNMockFirstNames(int n) {
        if (firstNames.get(n) == null) {
            List<String> names = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                names.add(faker.name().firstName());
            }
            firstNames.put(n, names);
        }
        return firstNames.get(n);
    }

    public static List<String> getNMockLastNames(int n) {
        if (lastNames.get(n) == null) {
            List<String> names = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                names.add(faker.name().lastName());
            }
            lastNames.put(n, names);
        }
        return lastNames.get(n);
    }

    public static Faker getSeededFaker() {
        return new Faker(new Random(0));
    }



}
