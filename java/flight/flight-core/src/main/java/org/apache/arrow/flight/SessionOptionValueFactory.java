/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import org.apache.arrow.flight.SessionOptionValue;

public class SessionOptionValueFactory {
    public static SessionOptionValue makeSessionOption(String value) {
        return new SessionOptionValueString(value);
    }

    public static SessionOptionValue makeSessionOption(bool value) {
        return new SessionOptionValueBool(value);
    }

    public static SessionOptionValue makeSessionOption(int value) {
        return new SessionOptionValueInt(value);
    }

    public static SessionOptionValue makeSessionOption(long value) {
        return new SessionOptionValueLong(value);
    }

    public static SessionOptionValue makeSessionOption(float value) {
        return new SessionOptionValueFloat(value);
    }

    public static SessionOptionValue makeSessionOption(double value) {
        return new SessionOptionValueDouble(value);
    }

    public static SessionOptionValue makeSessionOption(String[] value) {
        return new SessionOptionValueStringList(value);
    }
}

class SessionOptionValueString {
    private final String value;

    SessionOptionValue(String value) {
        this.value = value;
    }
}

class SessionOptionValueBool {
    private final boolean value;

    SessionOptionValue(boolean value) {
        this.value = value;
    }
}

class SessionOptionValueInt {
    private final int value;

    SessionOptionValue(int value) {
        this.value = value;
    }
}

class SessionOptionValueLong {
    private final long value;

    SessionOptionValue(long value) {
        this.value = value;
    }
}

class SessionOptionValueFloat {
    private final float value;

    SessionOptionValue(Float value) {
        this.value = value;
    }
}


class SessionOptionValueDouble {
    private final double value;

    SessionOptionValue(double value) {
        this.value = value;
    }
}
class SessionOptionValueStringList {
    private final String[] value;

    SessionOptionValue(String[] value) {
        this.value = value.clone();
    }
}
