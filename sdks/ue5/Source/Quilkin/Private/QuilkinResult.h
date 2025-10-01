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

#include "Misc/TVariant.h"

/* Equivalvent of Rust's `()` type to represent no-op operations */
struct TUnit {};

/*
* This is a port of Rust's Result type, in order to make it
* easier to define fallible functions without needing out parameters
*/
template <typename TValue, typename TError>
class TResult
{
public:
    ~TResult() = default;

    TResult(TValue InValue)
        : Data(TInPlaceType<TValue>(), InValue)
    {
    }

    TResult(TError InError)
        : Data(TInPlaceType<TError>(), InError)
    {
    }

    bool IsSuccess() const { return Data.template IsType<TValue>(); }
    bool IsError() const { return Data.template IsType<TError>(); }
    const TValue& GetValue() const { check(IsSuccess()); return Data.template Get<TValue>(); }
    const TError& GetError() const { check(IsError()); return Data.template Get<TError>(); }

private:
    TVariant<TValue, TError> Data;
};