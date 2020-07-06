package com.tianchi.django.common.enums;

import com.google.common.base.Enums;

public enum Resource {

    GPU, CPU, RAM, DISK;

    public static Resource to(String name) {
        return Enums.getIfPresent(Resource.class, name == null ? "" : name.toUpperCase()).orNull();
    }

}
