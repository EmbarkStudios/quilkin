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

#include "Containers/Deque.h"

/* A circular first-in last-out buffer used for taking the median of measurements */
template <typename T>
class CircularBuffer
{
public:
    explicit CircularBuffer(size_t InCapacity = 50)
        : Capacity(InCapacity)
    {
    }

    void Add(const T& value)
    {
        if (Buffer.Num() == Capacity)
        {
            Buffer.PopFirst();
        }
        Buffer.PushLast(value);
    }

    bool IsEmpty()
    {
        return Num() == 0;
    }

    size_t Num()
    {
        return Buffer.Num();
    }

    T Median() const
    {
        if (Buffer.IsEmpty())
        {
            return T{}; // Return default value if the buffer is empty
        }

        TArray<T> Sorted;
        for (const auto& Item : Buffer)
        {
            Sorted.Add(Item);
        }

        Sorted.Sort();

        size_t Middle = Sorted.Num() / 2;

        return (Sorted.Num() % 2 == 0) ? (Sorted[Middle] + Sorted[Middle - 1]) / 2 : Sorted[Middle];
    }

private:
    size_t Capacity;
    TDeque<T> Buffer;
};

