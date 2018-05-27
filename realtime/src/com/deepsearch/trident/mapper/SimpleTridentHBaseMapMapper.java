/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deepsearch.trident.mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.deepsearch.trident.state.StateUtils;

import backtype.storm.utils.Time;

public class SimpleTridentHBaseMapMapper implements TridentHBaseMapMapper {
    private String qualifier;
    private boolean isRealTime;

    public SimpleTridentHBaseMapMapper(String qualifier, boolean isRealTime) {
        this.qualifier = qualifier;
        this.isRealTime = isRealTime;
    }

    @Override
    public byte[] rowKey(List<Object> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
        		if(isRealTime){
        			bos.write(String.valueOf(StateUtils.formatHour(new Date(Time.currentTimeMillis()))).getBytes());
        		}
            for (Object key : keys) {
                bos.write(String.valueOf(key).getBytes());
            }
            bos.close();
        } catch (IOException e){
            throw new RuntimeException("IOException creating HBase row key.", e);
        }
        return bos.toByteArray();
    }

    @Override
    public String qualifier(List<Object> keys) {
        return this.qualifier;
    }
}
