// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module contains execution plans that are needed to distribute Datafusion's execution plans into
//! several Ballista executors.

mod query_stage;
mod shuffle_reader;
mod unresolved_shuffle;

pub use query_stage::QueryStageExec;
pub use shuffle_reader::ShuffleReaderExec;
pub use unresolved_shuffle::UnresolvedShuffleExec;
