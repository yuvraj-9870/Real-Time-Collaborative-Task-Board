# Real-Time-Collaborative-Task-Board

# Technical Deep-Dive: Real-Time Collaborative Task Board in Web &amp; Mobile Development Projects

In modern full-stack engineering, building software that updates instantly across multiple client sessions without manual browser refreshes is a fundamental requirement[1][2]. Within the **GDG-USAR Web &amp; Mobile Development Projects curriculum**, the **Real-Time Collaborative Task Board** serves as the definitive engineering framework for mastering **low-latency event-driven architecture, WebSocket pub/sub data pipelines, and concurrent state reconciliation**[1].

---

## 1\. The Real-Time Task Board in the Larger Context of Web &amp; Mobile Projects

The GDG-USAR Web &amp; Mobile Development curriculum is structured across four distinct engineering disciplines, each addressing a specialized technical domain[2]:

```
┌─────────────────────────────────────────────────────────────────────────┐
│              Web &amp; Mobile Development Engineering Portfolio             │
├────────────────────────────────┬────────────────────────────────────────┤
│ Project Domain                 │ Core Technical Focus                   │
├────────────────────────────────┼────────────────────────────────────────┤
│ 1. Interactive 3D Showcase     │ WebGL Render Loops, Draco Mesh         │
│    (3D Web &amp; Frontend)         │ Compression &amp; Raycasting [4, 7]     │
├────────────────────────────────┼────────────────────────────────────────┤
│ 2. File-Sharing Platform       │ Multi-Tenant Isolation, Row-Level      │
│    (Full-Stack Web &amp; Security) │ Security (RLS) &amp; Blob Streams [5, 8]│
├────────────────────────────────┼────────────────────────────────────────┤
│ 3. Collaborative Task Board    │ WebSocket Pub/Sub Channels, Postgres   │
│    (Realtime &amp; WebSockets)     │ CDC &amp; Concurrency Control [1, 2]     │
├────────────────────────────────┼────────────────────────────────────────┤
│ 4. Campus Utility App          │ Cross-Platform Mobile UX, On-Device    │
│    (Mobile Development)        │ Compression &amp; Offline Caching [6, 9]│
└────────────────────────────────┴────────────────────────────────────────┘

```

* **Interactive 3D Product Showcase**: Focuses on browser-based 3D graphics rendering using Three.js / React Three Fiber, optimizing frame rates on low-end GPUs via Device Pixel Ratio (DPR) capping, and converting 2D click coordinates into 3D world space using raycasting[4][7].
* **File-Sharing Platform**: Focuses on backend cloud security, managing multi-tenant file isolation using PostgreSQL Row-Level Security (RLS), streaming large uploads to prevent Node process memory exhaustion, and generating short-lived presigned URLs[5][8].
* **Campus Utility App**: Focuses on mobile client engineering, handling hardware integrations (Expo Camera/GPS), keyboard obscuration, and persistent local storage rehydration with graceful offline degradation[6][9].
* **Real-Time Collaborative Task Board**: Focuses on **distributed state synchronization**[1][2]. While standard web applications operate on synchronous HTTP request-response cycles, the Task Board transitions to an **asynchronous, full-duplex event-driven model** where state mutations propagate across independent browser sessions in under 100ms[1][3].

---

## 2\. Core Architectural Blueprint, Tech Stack &amp; Schema

### Problem Statement &amp; Architectural Need

Traditional Kanban board implementations suffer from stale client data[1][2]. When multiple team members interact with the same board, changes made by User A are invisible to User B until User B manually reloads the page or triggers a background HTTP polling request[1][2]. HTTP polling introduces severe network overhead and fails to prevent race conditions during simultaneous edits[1][2].

To solve this, the architecture combines a **smooth drag-and-drop client interface** with an **event-driven pub/sub data pipeline**[1][3].

```
[ User Action: Drag Card / Edit Task ]
                  │
                  ▼
┌───────────────────────────────────────────────────────────┐
│ Module 1: Local Optimistic UI State                       │
│  ├── Instantly reorders card in local React state         │
│  └── Renders UI update immediately (&lt; 16ms frame target)  │
└────────────────────────┬──────────────────────────────────┘
                         │
                         ▼
┌───────────────────────────────────────────────────────────┐
│ Module 2: Mutation &amp; Concurrency Guard                    │
│  ├── Submits async UPDATE/INSERT payload to PostgreSQL    │
│  └── Evaluates Optimistic Concurrency Control (version ID)│
└────────────────────────┬──────────────────────────────────┘
                         │
                         ▼
┌───────────────────────────────────────────────────────────┐
│ Module 3: Postgres Logical Replication / Realtime Hub     │
│  ├── Database commits SQL mutation row                    │
│  └── Change-Data-Capture (CDC) converts change to WebSocket│
└────────────────────────┬──────────────────────────────────┘
                         │
                         ▼
┌───────────────────────────────────────────────────────────┐
│ Module 4: Remote Subscriber Browser Windows               │
│  ├── WebSocket receives `postgres_changes` event payload  │
│  ├── Merges delta payload into local component state      │
│  └── Re-renders card position dynamically without refresh │
└───────────────────────────────────────────────────────────┘

```

### Technology Stack Selection

* **Framework**: Next.js 14+ (App Router) for hybrid client/server rendering[10].
* **Realtime Engine**: Supabase Realtime (or self-hosted Socket.IO), utilizing managed WebSockets running over PostgreSQL Logical Replication / Change-Data-Capture (CDC)[3][10].
* **Database**: PostgreSQL with native CDC publication support[10][11].
* **Drag-and-Drop Handler**: `@hello-pangea/dnd` (React 18+ fork of `react-beautiful-dnd`) for accessible card drag operations[10].
* **Styling**: Tailwind CSS for responsive multi-column layouts[10].

### Relational Database Schema &amp; CDC Enablers

The underlying PostgreSQL database schema must support atomic position reordering and concurrency tracking[11]:

```
-- Enable UUID extension
create extension if not exists "uuid-ossp";

-- Custom Task Status Enum
create type task_status as enum ('TODO', 'IN_PROGRESS', 'COMPLETED');

-- Tasks Table Schema
create table public.tasks (
  id uuid default uuid_generate_v4() primary key,
  title text not null,
  description text default '',
  status task_status default 'TODO'::task_status not null,
  position integer default 0 not null, -- Stores column ordering sequence
  assignee text default 'Unassigned',
  version integer default 1 not null, -- Used for Concurrency Control (OCC)
  updated_at timestamp with time zone default timezone('utc'::text, now()) not null,
  created_at timestamp with time zone default timezone('utc'::text, now()) not null
);

-- Index for status-based column queries
create index idx_tasks_status on public.tasks(status);

-- CRITICAL: Enable Postgres Logical Replication for Realtime Pub/Sub Broadcasting
alter publication supabase_realtime add table public.tasks;

```

---

## 3\. Code Breakdown &amp; Underlying Logic

The core logic of the collaborative board revolves around managing dual state sources: **Local Component State** (for responsive rendering) and **Remote Server State** (pushed via WebSockets)[3].

### Foundational React + Realtime Synchronization Implementation

```
'use client';

import React, { useEffect, useState } from 'react';
import { createClient } from '@supabase/supabase-js';
import { DragDropContext, Droppable, Draggable, DropResult } from '@hello-pangea/dnd';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || '';
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';
const supabase = createClient(supabaseUrl, supabaseAnonKey);

type StatusType = 'TODO' | 'IN_PROGRESS' | 'COMPLETED';

interface Task {
  id: string;
  title: string;
  status: StatusType;
  position: number;
  version: number;
}

export default function KanbanBoard() {
  const [tasks, setTasks] = useState
```
1. Initial Data Fetch &amp; Realtime Subscription Setup
  useEffect(() =&gt; {
    // Fetch initial board state on mount
    const fetchTasks = async () =&gt; {
      const { data } = await supabase
        .from('tasks')
        .select('*')
        .order('position', { ascending: true });
      if (data) setTasks(data);
    };

    fetchTasks();

    // Establish WebSocket Channel Connection
    const channel = supabase
      .channel('realtime_kanban')
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table: 'tasks' },
        (payload) =&gt; {
          handleRealtimeEvent(payload);
        }
      )
      .subscribe();

    // Cleanup subscription on unmount
    return () =&gt; {
      supabase.removeChannel(channel);
    };
  }, []);

  // 2. Incoming Realtime Event Dispatcher Logic
  const handleRealtimeEvent = (payload: any) =&gt; {
    const { eventType, new: newRow, old: oldRow } = payload;

    setTasks((prevTasks) =&gt; {
      if (eventType === 'INSERT') {
        // Prevent duplicate append if local client already created it optimistically
        if (prevTasks.some((t) =&gt; t.id === newRow.id)) return prevTasks;
        return [...prevTasks, newRow];
      }

      if (eventType === 'UPDATE') {
        return prevTasks.map((task) =&gt;
          task.id === newRow.id ? { ...task, ...newRow } : task
        );
      }

      if (eventType === 'DELETE') {
        return prevTasks.filter((task) =&gt; task.id !== oldRow.id);
      }

      return prevTasks;
    });
  };

  // 3. Drag End Handler with Optimistic UI Update &amp; DB Persist
  const onDragEnd = async (result: DropResult) =&gt; {
    const { destination, source, draggableId } = result;

    if (!destination) return;
    if (
      destination.droppableId === source.droppableId &amp;&amp;
      destination.index === source.index
    ) return;

    const targetStatus = destination.droppableId as StatusType;

    // Preserve previous state for rollback on error
    const previousTasks = [...tasks];

    // OPTIMISTIC UPDATE: Update React State Immediately
    setTasks((prevTasks) =&gt;
      prevTasks.map((task) =&gt; {
        if (task.id === draggableId) {
          return {
            ...task,
            status: targetStatus,
            position: destination.index,
            version: task.version + 1,
          };
        }
        return task;
      })
    );

    // PERSIST MUTATION TO DATABASE
    const { error } = await supabase
      .from('tasks')
      .update({
        status: targetStatus,
        position: destination.index,
        version: previousTasks.find((t) =&gt; t.id === draggableId)!.version + 1,
        updated_at: new Date().toISOString(),
      })
      .eq('id', draggableId);

    // ROLLBACK LOGIC: Revert UI state if backend write fails
    if (error) {
      console.error('Mutation failed, rolling back:', error.message);
      setTasks(previousTasks);
    }
  };

  return (
    
  );
}

```

### Detailed Breakdown of Key Technical Mechanisms

1. **Optimistic UI Updates**: To maintain a 60 FPS user experience, the `onDragEnd` handler reorders the client's local React state **immediately** upon releasing a card[1]. It does not wait for the asynchronous HTTP request to complete across the network[3][14].
2. **PostgreSQL Change-Data-Capture (CDC)**: When `supabase.from('tasks').update()` hits PostgreSQL, the database engine writes the change to its Write-Ahead Log (WAL)[3]. The Supabase Realtime engine listens to the WAL via logical replication and broadcasts a `postgres_changes` JSON event over open WebSockets[3][10].
3. **Preventing Echo / Broadcast Loops**: A major bug in WebSocket engineering occurs when Window A receives an inbound socket event about its own edit and triggers a second local state mutation, creating an infinite broadcast loop[15][16]. This is mitigated by:
  * **Server-Side Sender Filtering**: Using `socket.broadcast.emit()` on self-hosted WebSocket servers, transmitting events to all clients *except* the socket originating the change[15][16].
  * **Client Origin Identification**: Attaching a unique session ID (`clientOriginId`) to mutation payloads so clients ignore inbound events matching their own ID[15].
  * **State Deduplication Filters**: Checking `if (prevTasks.some(t =&gt; t.id === newRow.id))` inside event handlers before executing state updates[12][13].
4. **Concurrent Conflict Resolution**: When User A moves a task to "Completed" while User B simultaneously deletes that exact task, a race condition occurs[15]:
  * User A updates the UI optimistically[17].
  * User B's delete command reaches PostgreSQL first, purging the record ID[17].
  * User A's update fails on the server with a 404/Zero Rows Modified error[17].
  * The server pushes a `STATE_RESYNC` or `DELETE` event to User A, whose client executes a state rollback, removing the deleted card cleanly from User A's screen[17].

---

## 4\. Impact Analysis: What Changes if Small Code Alterations Are Made?

In real-time systems, minor adjustments to network parameters, state handlers, or database directives completely alter system behavior.

```
┌─────────────────────────────────────────┬─────────────────────────────────────────┐
│ Small Code Alteration                   │ Resulting System Impact &amp; Failure Mode │
├─────────────────────────────────────────┼─────────────────────────────────────────┤
│ 1. Removing Sender Filtering / Origin   │ Causes Infinite Synchronization / Echo  │
│    ID Check from WebSocket handlers     │ Loops across connected clients [15, 16]│
├─────────────────────────────────────────┼─────────────────────────────────────────┤
│ 2. Switching payload from Differential  │ Severe Network Bloat and Overwriting    │
│    Deltas to Full Board State JSON      │ Unrelated Concurrent Column Edits [18]  │
├─────────────────────────────────────────┼─────────────────────────────────────────┤
│ 3. Removing Local Optimistic UI State   │ Introduces UI Drag-and-Drop Lag         │
│    Updates before network writes        │ Dependent on Network RTT Latency [3]   │
├─────────────────────────────────────────┼─────────────────────────────────────────┤
│ 4. Omitting the State Rollback Handler  │ Leaves UI Desynchronized from Database  │
│    in `onDragEnd` catch blocks          │ upon Network Write Failures [14, 17]    │
├─────────────────────────────────────────┼─────────────────────────────────────────┤
│ 5. Omitting `alter publication          │ Realtime WebSockets Fail Silently;     │
│    supabase_realtime add table ...`     │ App Reverts to Stale Static View [11]   │
└─────────────────────────────────────────┴─────────────────────────────────────────┘

```

### 1\. Removing Sender Filtering / Client Origin Tracking

* **Code Modification**: Removing the check that filters out self-generated WebSocket events, or using `io.emit()` instead of `socket.broadcast.emit()` on the server[15][16].
* **Resulting Change**:
  1. Window A moves Task #101.
  2. Window A updates local state and sends `UPDATE` to the server[15].
  3. The server broadcasts `TASK_UPDATED` back to **all** clients, including Window A[15].
  4. Window A receives its own event, triggers a re-render, and re-fires its local state effect, causing an **infinite event loop** that crashes the browser UI thread and floods the server with socket frames[15][16].

---

### 2\. Switching from Differential Deltas to Full Board State JSON

* **Code Modification**: Changing the WebSocket payload from a differential delta (`{ taskId: "88", status: "COMPLETED" }`) to serializing the entire board state (`{ tasks: [ ...500 tasks... ] }`)[16].
* **Resulting Change**:
  * **Payload Overhead**: Network bandwidth consumption increases exponentially from `&lt; 1KB` per action to `50KB–500KB+` per drag action[18][19].
  * **Destructive Overwrites**: If User A moves a card in Column 1 while User B edits a title in Column 3, broadcasting full board snapshots causes whichever payload arrives second to **overwrite the unrelated edits made by the other user**, resulting in silent data loss[16].

---

### 3\. Removing Local Optimistic UI Updates

* **Code Modification**: Awaiting the network request before updating local React state:

```
// DANGEROUS MODIFICATION: Waiting for network round-trip
const onDragEnd = async (result: DropResult) =&gt; {
  // UI remains frozen while network call executes
  await supabase.from('tasks').update({ status: targetStatus }).eq('id', draggableId);
  // UI updates only after server response
  setTasks(fetchedTasks); 
};

```

* **Resulting Change**: The user experiences severe **UI drag latency**[1][3]. On mobile connections or high-latency Wi-Fi (e.g., 150ms Round-Trip Time), dragged cards freeze in mid-air before jumping into the destination column, violating modern responsive UX standards[1][3].

---

### 4\. Omitting State Rollback Handlers on Network Failure

* **Code Modification**: Removing the error catch block and `previousTasks` restoration from the drag handler[14][17].
* **Resulting Change**: If a student loses network connectivity mid-drag, the UI optimistically reflects the card in the "Completed" column[14]. However, because the backend write failed silently, the database still holds the card under "To Do"[17]. When the user reloads the browser, the card suddenly jumps back to "To Do", causing **ghost state anomalies and user confusion**[14][17].

---

### 5\. Omitting Database CDC Publication Directives

* **Code Modification**: Creating the `tasks` SQL table but forgetting to execute `alter publication supabase_realtime add table public.tasks;`[11].
* **Resulting Change**: The database executes standard SQL CRUD operations normally, but PostgreSQL's logical replication engine **does not capture row deltas** for the table[3][11]. The WebSocket server remains connected but never broadcasts `postgres_changes` events, causing the real-time collaborative board to silently degrade into a **static, single-user application**[1].

---

## 5\. Summary Architectural Comparison Matrix

| Feature Dimension             | Full State Snapshot Broadcast                                 | Atomic Differential Delta Payload                     |
| ----------------------------- | ------------------------------------------------------------- | ----------------------------------------------------- |
| **Network Payload Size**      | High (Scales with total board tasks, e.g., 50KB+)[18][19] | Minimal (`&lt; 1KB` per action)[18][19]              |
| **Concurrent Conflict Risk**  | Severe (Overwrites concurrent column edits)[18][19]       | Low (Mutates specific record fields only)[18][19] |
| **Implementation Complexity** | Low (Simple full state replacement)[19]                     | Moderate (Requires reducer/event handlers)[19]      |
| **Bandwidth Scalability**     | Degrades rapidly under multi-user activity[18]              | Highly scalable over standard WebSockets[18]        |
| **User Experience**           | Frequent UI flashes and overwritten input[18]               | Smooth, non-intrusive realtime card updates[1][3] |
