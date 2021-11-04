package ru.stepan;

import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.Random;

@lombok.Data
@AllArgsConstructor
@Builder
public class Data {

    public Data() {
        Random random = new Random();
        this.num = 100000 + random.nextInt(900000);
        this.code = 1000 + random.nextInt(9000);

        int lenght = 4 + random.nextInt(6);
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < lenght; i++) {
            stringBuilder.append((char) (97 + random.nextInt(26)));
        }

        this.someString = stringBuilder.toString();
    }

    int num;
    int code;
    String someString;
}
