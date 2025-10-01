/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

/** A `TMap` data structure wrapped with `FRWLock` to allow for thread safe 
  * concurrent access.
  */
template<typename TKey, typename TValue>
class TSConcurrentMap
{
public:
    TSConcurrentMap() = default;

    ~TSConcurrentMap() { Empty(); }

    /** Resets the map with the specified keysand calls `Func` with each key for
      * generating the value to be inserted into the map.
      */
    template <typename Fn> void ResetWithKeys(const TArray<TKey>& NewKeys, Fn Func)
    {
        RWLock.WriteLock();
        DataMap.Empty();

        for (const TKey& Endpoint : NewKeys)
        {
            TValue DefaultValue = Func(&Endpoint);
            DataMap.Add(Endpoint, DefaultValue);
        }
        RWLock.WriteUnlock();
    }

    /** If `Key` is not present, inserts `Value` into the map, otherwise it updates
      * the existing `Key` entry.
      */
    void AddOrUpdate(const TKey& Key, const TValue& Value)
    {
        RWLock.WriteLock();
        TValue* FoundValue = DataMap.Find(Key);
        if (FoundValue)
        {
            *FoundValue = Value;
        }
        else
        {
            DataMap.Add(Key, Value);
        }
        RWLock.WriteUnlock();
    }

    /** If `Key` is not present, inserts `Value` into the map, otherwise it has
      * no effect.
      */
    void Add(const TKey& Key, const TValue& Value)
    {
        RWLock.WriteLock();
        DataMap.Add(Key, Value);
        RWLock.WriteUnlock();
    }

    /** Returns the pointer to the value matching `Key`, if present, otherwise
      * returns a null pointer.
      */
    const TValue* Find(const TKey& Key) const
    {
        RWLock.ReadLock();
        const TValue* Result = DataMap.Find(Key);
        RWLock.ReadUnlock();

        return Result;
    }

    /** Finds or inserts a default `TValue` in `Key`, and then calls `Add` on
      * `TValue` with `Latency`.
      */
    void FindOrDefaultToAdd(const TKey& Key, int64 Latency)
    {
        RWLock.WriteLock();
        DataMap.FindOrAdd(Key, TValue()).Add(Latency);
        RWLock.WriteUnlock();
    }

    /** Removes the entry matching `Key`, if present. */
    void Remove(const TKey& Key)
    {
        RWLock.WriteLock();
        DataMap.Remove(Key);
        RWLock.WriteUnlock();
    }

    /** Returns the number of entries in the map. */
    int32 Num() const
    {
        RWLock.ReadLock();
        auto Num = DataMap.Num();
        RWLock.ReadUnlock();
        return Num;
    }

    /** Returns whether the map contains no entries. */
    bool IsEmpty() const
    {
        RWLock.ReadLock();
        auto Empty = DataMap.IsEmpty();
        RWLock.ReadUnlock();
        return Empty;
    }

    /** Returns whether the map contains an entry matching `Key`. */
    bool Contains(const TKey& Key) const
    {
        RWLock.ReadLock();
        auto Result = DataMap.Contains(Key);
        RWLock.ReadUnlock();
        return Result;
    }

    /** Removes all entries from the map. */
    void Empty()
    {
        RWLock.WriteLock();
        DataMap.Empty();
        RWLock.WriteUnlock();
    }

    /** Returns all keys from the map. */
    TArray<TKey> GetKeys() const
    {
        RWLock.ReadLock();
        TArray<TKey> Keys;
        DataMap.GetKeys(Keys);
        RWLock.ReadUnlock();
        return Keys;
    }

    /** Accepts a closure which accepts (KEY, VALUE) and returns a ENTRY.
      *
      * SAFETY: The closure must not call any method which write locks this map, otherwise
      * it will cause re-entrance.
      */
    template<typename ENTRY, typename Fn>
    TArray<ENTRY> MapToArray(Fn Closure) const
    {
        TArray<ENTRY> Entries;
        RWLock.ReadLock();
        for (auto& Entry : DataMap) {
            Entries.Push(Closure(Entry.template Get<0>(), Entry.template Get<1>()));
        }
        RWLock.ReadUnlock();
        return Entries;
    }

    /** Accepts a closure which accepts (KEY, VALUE) and returns a TOptional<ENTRY>.
      * If TOptional is not set, then that entry is not included in the returned array.
      *
      * SAFETY: The closure must not call any method which write locks this map, otherwise
      * it will cause re-entrance.
      */
    template<typename ENTRY, typename Fn>
    TArray<ENTRY> FilterMapToArray(Fn Closure) const
    {
        TArray<ENTRY> Entries;
        RWLock.ReadLock();
        for (auto& Entry : DataMap) {
            TOptional<ENTRY> Option = Closure(Entry.template Get<0>(), Entry.template Get<1>());

            if (Option.IsSet()) {
                Entries.Push(Option.GetValue());
            }
        }
        RWLock.ReadUnlock();
        return Entries;
    }

    /** Accepts a closure which accepts (KEY, VALUE) and returns void.
      *
      * SAFETY: The closure must not call any method which write locks this map, otherwise
      * it will cause re-entrance.
      */
    template <typename Fn> void ForEach(Fn Func) const
    {
        RWLock.ReadLock();
        for (const TPair<TKey, TValue>& Pair : DataMap)
        {
            Func(Pair.template Get<0>(), Pair.template Get<1>());
        }
        RWLock.ReadUnlock();
    }

private:
    TMap<TKey, TValue> DataMap;
    mutable FRWLock RWLock;
};

