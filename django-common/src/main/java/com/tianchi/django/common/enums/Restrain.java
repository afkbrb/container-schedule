package com.tianchi.django.common.enums;

import com.google.common.base.Enums;

public enum Restrain {

    LE,//小于或等于
    GE;//大于或等于

    public static Restrain to(String name) {
        return Enums.getIfPresent(Restrain.class, name == null ? "" : name.toUpperCase()).orNull();
    }

}
