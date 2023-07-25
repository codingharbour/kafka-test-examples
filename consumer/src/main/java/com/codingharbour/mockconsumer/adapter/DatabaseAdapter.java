package com.codingharbour.mockconsumer.adapter;

import com.codingharbour.mockconsumer.EventData;

import java.util.List;

public interface DatabaseAdapter {
    void save(List<EventData> eventDataList);
}
