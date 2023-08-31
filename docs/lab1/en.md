# Introduction

There are two components here, one is coordinator and another is worker. If we want to keep worker's status in coordinator, it will be complex here. In contrast, we let worker keep track of health status of the coordinator.

If coodirnator is health, worker will try to retrive the task from it. After coordinator dipatch all the map & reduce task, it's status turn into unhealth. When worker receives the unhealth coordinator, work exits the program and leave.