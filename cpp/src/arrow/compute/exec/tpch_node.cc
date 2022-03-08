#include "arrow/compute/exec/tpch_node.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/future.h"
#include "arrow/util/unreachable.h"

#include <algorithm>
#include <bitset>
#include <cstring>
#include <random>
#include <vector>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_set>

namespace arrow
{
    using internal::checked_cast;

    namespace compute
    {
        class TpchText
        {
        public:
            Status InitIfNeeded(random::pcg32_fast &rng);
            Result<Datum> GenerateComments(
                size_t num_comments,
                size_t min_length,
                size_t max_length,
                random::pcg32_fast &rng);

        private:
            bool GenerateWord(int64_t &offset, random::pcg32_fast &rng, char *arr, const char **words, size_t num_choices);
            bool GenerateNoun(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GenerateVerb(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GenerateAdjective(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GenerateAdverb(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GeneratePreposition(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GenerateAuxiliary(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GenerateTerminator(int64_t &offset, random::pcg32_fast &rng, char *arr);

            bool GenerateNounPhrase(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GenerateVerbPhrase(int64_t &offset, random::pcg32_fast &rng, char *arr);
            bool GeneratePrepositionalPhrase(int64_t &offset, random::pcg32_fast &rng, char *arr);

            bool GenerateSentence(int64_t &offset, random::pcg32_fast &rng, char *arr);

            std::atomic<bool> done_ = { false };
            int64_t generated_offset_ = 0;
            std::mutex text_guard_;
            std::unique_ptr<Buffer> text_;
            static constexpr int64_t kChunkSize = 8192;
            static constexpr int64_t kTextBytes = 300 * 1024 * 1024; // 300 MB
        };

        class TpchTableGenerator
        {
        public:
            using OutputBatchCallback = std::function<void(ExecBatch)>;
            using FinishedCallback = std::function<void(int64_t)>;
            using GenerateFn = std::function<Status(size_t)>;
            using ScheduleCallback = std::function<Status(GenerateFn)>;
            using AbortCallback = std::function<void()>;

            virtual Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) = 0;

            virtual Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) = 0;

            void Abort(AbortCallback abort_callback)
            {
                bool expected = false;
                if(done_.compare_exchange_strong(expected, true))
                {
                    abort_callback();
                }
            }

            virtual std::shared_ptr<Schema> schema() const = 0;

            virtual ~TpchTableGenerator() = default;

        protected:
            std::atomic<bool> done_ = { false };
            std::atomic<int64_t> batches_outputted_ = { 0 };
        };

        int GetNumDigits(int64_t x)
        {
            // This if statement chain is for MAXIMUM SPEED
            /*
              .,
              .      _,'f----.._
              |\ ,-'"/  |     ,'
              |,_  ,--.      /
              /,-. ,'`.     (_
              f  o|  o|__     "`-.
              ,-._.,--'_ `.   _.,-`
              `"' ___.,'` j,-'
              `-.__.,--'
             */
            // Source: https://stackoverflow.com/questions/1068849/how-do-i-determine-the-number-of-digits-of-an-integer-in-c
            ARROW_DCHECK(x >= 0);
            if(x < 10ll) return 1;
            if(x < 100ll) return 2;
            if(x < 1000ll) return 3;
            if(x < 10000ll) return 4;
            if(x < 100000ll) return 5;
            if(x < 1000000ll) return 6;
            if(x < 10000000ll) return 7;
            if(x < 100000000ll) return 8;
            if(x < 1000000000ll) return 9;
            if(x < 10000000000ll) return 10;
            if(x < 100000000000ll) return 11;
            if(x < 1000000000000ll) return 12;
            if(x < 10000000000000ll) return 13;
            if(x < 100000000000000ll) return 14;
            if(x < 1000000000000000ll) return 15;
            if(x < 10000000000000000ll) return 16;
            if(x < 100000000000000000ll) return 17;
            if(x < 1000000000000000000ll) return 18;
            return -1;
        }

        void AppendNumberPaddedToNineDigits(char *out, int64_t x)
        {
            // We do all of this to avoid calling snprintf, which does a lot of crazy
            // locale stuff. On Windows and MacOS this can get suuuuper slow
            int num_digits = GetNumDigits(x);
            int num_padding_zeros = std::max(9 - num_digits, 0);
            std::memset(out, '0', static_cast<size_t>(num_padding_zeros));
            while(x > 0)
            {
                *(out + num_padding_zeros + num_digits - 1) = ('0' + x % 10);
                num_digits -= 1;
                x /= 10;
            }
        }

        Result<std::shared_ptr<Schema>> SetOutputColumns(
            const std::vector<std::string> &columns,
            const std::vector<std::shared_ptr<DataType>> &types,
            const std::unordered_map<std::string, int> &name_map,
            std::vector<int> &gen_list)
        {
            gen_list.clear();
            std::vector<std::shared_ptr<Field>> fields;
            if(columns.empty())
            {
                fields.resize(name_map.size());
                gen_list.resize(name_map.size());
                for(auto pair : name_map)
                {
                    int col_idx = pair.second;
                    fields[col_idx] = field(pair.first, types[col_idx]);
                    gen_list[col_idx] = col_idx;
                }
                return schema(std::move(fields));
            }
            else
            {
                for(const std::string &col : columns)
                {
                    auto entry = name_map.find(col);
                    if(entry == name_map.end())
                        return Status::Invalid("Not a valid column name");
                    int col_idx = static_cast<int>(entry->second);
                    fields.push_back(field(col, types[col_idx]));
                    gen_list.push_back(col_idx);
                }
                return schema(std::move(fields));
            }
        }

        static TpchText g_text;

        Status TpchText::InitIfNeeded(random::pcg32_fast &rng)
        {
            if(done_.load())
                return Status::OK();

            {
                std::lock_guard<std::mutex> lock(text_guard_);
                if(!text_)
                {
                    ARROW_ASSIGN_OR_RAISE(text_, AllocateBuffer(kTextBytes));
                }
            }
            char *out = reinterpret_cast<char *>(text_->mutable_data());
            char temp_buff[kChunkSize];
            while(done_.load() == false)
            {
                int64_t known_valid_offset = 0;
                int64_t try_offset = 0;
                while(GenerateSentence(try_offset, rng, temp_buff))
                    known_valid_offset = try_offset;

                {
                    std::lock_guard<std::mutex> lock(text_guard_);
                    if(done_.load())
                        return Status::OK();
                    int64_t bytes_remaining = kTextBytes - generated_offset_;
                    int64_t memcpy_size = std::min(known_valid_offset, bytes_remaining);
                    std::memcpy(out + generated_offset_, temp_buff, memcpy_size);
                    generated_offset_ += memcpy_size;
                    if(generated_offset_ == kTextBytes)
                        done_.store(true);
                }
            }
            return Status::OK();
        }

        Result<Datum> TpchText::GenerateComments(
            size_t num_comments,
            size_t min_length,
            size_t max_length,
            random::pcg32_fast &rng)
        {
            RETURN_NOT_OK(InitIfNeeded(rng));
            std::uniform_int_distribution<size_t> length_dist(min_length, max_length);
            ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> offset_buffer, AllocateBuffer(sizeof(int32_t) * (num_comments + 1)));
            int32_t *offsets = reinterpret_cast<int32_t *>(offset_buffer->mutable_data());
            offsets[0] = 0;
            for(size_t i = 1; i <= num_comments; i++)
                offsets[i] = offsets[i - 1] + length_dist(rng);

            ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> comment_buffer, AllocateBuffer(offsets[num_comments]));
            char *comments = reinterpret_cast<char *>(comment_buffer->mutable_data());
            for(size_t i = 0; i < num_comments; i++)
            {
                size_t length = offsets[i + 1] - offsets[i];
                std::uniform_int_distribution<size_t> offset_dist(0, kTextBytes - length);
                size_t offset_in_text = offset_dist(rng);
                std::memcpy(comments + offsets[i], text_->data() + offset_in_text, length);
            }
            ArrayData ad(utf8(), num_comments, { nullptr, std::move(offset_buffer), std::move(comment_buffer) });
            return std::move(ad);
        }

        Result<Datum> RandomVString(
            random::pcg32_fast &rng,
            int64_t num_rows,
            int32_t min_length,
            int32_t max_length)
        {
            std::uniform_int_distribution<int32_t> length_dist(min_length, max_length);
            ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> offset_buff, AllocateBuffer((num_rows + 1) * sizeof(int32_t)));
            int32_t *offsets = reinterpret_cast<int32_t *>(offset_buff->mutable_data());
            offsets[0] = 0;
            for(int64_t i = 1; i <= num_rows; i++)
                offsets[i] = offsets[i - 1] + length_dist(rng);
            ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> str_buff, AllocateBuffer(offsets[num_rows]));
            char *str = reinterpret_cast<char *>(str_buff->mutable_data());

            // Spec says to pick random alphanumeric characters from a set of at least
            // 64 symbols. Now, let's think critically here: 26 letters in the alphabet,
            // so 52 total for upper and lower case, and 10 possible digits gives 62
            // characters...
            // dbgen solves this by including a space and a comma as well, so we'll
            // copy that.
            const char alpha_numerics[65] =
                "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";
            std::uniform_int_distribution<int> char_dist(0, 63);
            for(int32_t i = 0; i < offsets[num_rows]; i++)
                str[i] = alpha_numerics[char_dist(rng)];

            ArrayData ad(utf8(), num_rows, { nullptr, std::move(offset_buff), std::move(str_buff) });
            return std::move(ad);
        }

        void AppendNumber(char *&out, int num_digits, int32_t x)
        {
            out += (num_digits - 1);
            while(x > 0)
            {
                *out-- = '0' + (x % 10);
                x /= 10;
            }
            out += (num_digits + 1);
        }

        void GeneratePhoneNumber(
            char *out,
            random::pcg32_fast &rng,
            int32_t country)
        {
            std::uniform_int_distribution<int32_t> three_digit(100, 999);
            std::uniform_int_distribution<int32_t> four_digit(1000, 9999);

            int32_t country_code = country + 10;
            int32_t l1 = three_digit(rng);
            int32_t l2 = three_digit(rng);
            int32_t l3 = four_digit(rng);
            AppendNumber(out, 2, country_code);
            *out++ = '-';
            AppendNumber(out, 3, l1);
            *out++ = '-';
            AppendNumber(out, 3, l2);
            *out++ = '-';
            AppendNumber(out, 4, l3);
        }

        static constexpr uint32_t STARTDATE = 8035; // January 1, 1992 is 8035 days after January 1, 1970
        static constexpr uint32_t CURRENTDATE = 9298; // June 17, 1995 is 9298 days after January 1, 1970
        static constexpr uint32_t ENDDATE = 10591; // December 12, 1998 is 10591 days after January 1, 1970

        const char *NameParts[] =
        {
            "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
            "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
            "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
            "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
            "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
            "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
            "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
            "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
            "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
            "wheat", "white", "yellow",
        };
        static constexpr size_t kNumNameParts = sizeof(NameParts) / sizeof(NameParts[0]);

        const char *Types_1[] =
        {
            "STANDARD ", "SMALL ", "MEDIUM ", "LARGE ", "ECONOMY ", "PROMO ",
        };
        static constexpr size_t kNumTypes_1 = sizeof(Types_1) / sizeof(Types_1[0]);

        const char *Types_2[] =
        {
            "ANODIZED ", "BURNISHED ", "PLATED ", "POLISHED ", "BRUSHED ",
        };
        static constexpr size_t kNumTypes_2 = sizeof(Types_2) / sizeof(Types_2[0]);

        const char *Types_3[] =
        {
            "TIN", "NICKEL", "BRASS", "STEEL", "COPPER",
        };
        static constexpr size_t kNumTypes_3 = sizeof(Types_3) / sizeof(Types_3[0]);

        const char *Containers_1[] =
        {
            "SM ", "LG ", "MD ", "JUMBO ", "WRAP ",
        };
        static constexpr size_t kNumContainers_1 = sizeof(Containers_1) / sizeof(Containers_1[0]);

        const char *Containers_2[] =
        {
            "CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM",
        };
        static constexpr size_t kNumContainers_2 = sizeof(Containers_2) / sizeof(Containers_2[0]);

        const char *Segments[] =
        {
            "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD",
        };
        static constexpr size_t kNumSegments = sizeof(Segments) / sizeof(Segments[0]);

        const char *Priorities[] =
        {
            "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW",
        };
        static constexpr size_t kNumPriorities = sizeof(Priorities) / sizeof(Priorities[0]);

        const char *Instructions[] =
        {
            "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN",
        };
        static constexpr size_t kNumInstructions = sizeof(Instructions) / sizeof(Instructions[0]);

        const char *Modes[] =
        {
            "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB",
        };
        static constexpr size_t kNumModes = sizeof(Modes) / sizeof(Modes[0]);

        const char *Nouns[] =
        {
            "foxes ", "ideas ", "theodolites ", "pinto beans ", "instructions ", "dependencies ", "excuses ",
            "platelets ", "asymptotes ", "courts ", "dolphins ", "multipliers ", "sautemes ", "warthogs ", "frets ",
            "dinos ", "attainments ", "somas ", "Tiresias '", "patterns ", "forges ", "braids ", "hockey players ", "frays ",
            "warhorses ", "dugouts ", "notomis ", "epitaphs ", "pearls ", "tithes ", "waters ", "orbits ", "gifts ", "sheaves ",
            "depths ", "sentiments ", "decoys ", "realms ", "pains ", "grouches ", "escapades ",
        };
        static constexpr size_t kNumNouns = sizeof(Nouns) / sizeof(Nouns[0]);

        const char *Verbs[] =
        {
            "sleep ", "wake ", "are ", "cajole ", "haggle ", "nag ", "use ", "boost ", "affix ", "detect ", "integrate ",
            "maintain ", "nod ", "was ", "lose ", "sublate ", "solve ", "thrash ", "promise ", "engage ", "hinder ",
            "print ", "x-ray ", "breach ", "eat ", "grow ", "impress ", "mold ", "poach ", "serve ", "run ", "dazzle ",
            "snooze ", "doze ", "unwind ", "kindle ", "play ", "hang ", "believe ", "doubt ",
        };
        static constexpr size_t kNumVerbs = sizeof(Verbs) / sizeof(Verbs[0]);

        const char *Adjectives[] =
        {
            "furious ", "sly ", "careful ", "blithe ", "quick ", "fluffy ", "slow ", "quiet ", "ruthless ", "thin ",
            "close ", "dogged ", "daring ", "brave ", "stealthy ", "permanent ", "enticing ", "idle ", "busy ",
            "regular ", "final ", "ironic ", "even ", "bold ", "silent ",
        };
        static constexpr size_t kNumAdjectives = sizeof(Adjectives) / sizeof(Adjectives[0]);

        const char *Adverbs[] =
        {
            "sometimes ", "always ", "never ", "furiously ", "slyly ", "carefully ", "blithely ", "quickly ", "fluffily ",
            "slowly ", "quietly ", "ruthlessly ", "thinly ", "closely ", "doggedly ", "daringly ", "bravely ", "stealthily ",
            "permanently ", "enticingly ", "idly ", "busily ", "regularly ", "finally ", "ironically ", "evenly ", "boldly ",
            "silently ",
        };
        static constexpr size_t kNumAdverbs = sizeof(Adverbs) / sizeof(Adverbs[0]);

        const char *Prepositions[] =
        {
            "about ", "above ", "according to ", "across ", "after ", "against ", "along ", "alongside of ", "among ",
            "around ", "at ", "atop ", "before ", "behind ", "beneath ", "beside ", "besides ", "between ", "beyond ",
            "beyond ", "by ", "despite ", "during ", "except ", "for ", "from ", "in place of ", "inside ", "instead of ",
            "into ", "near ", "of ", "on ", "outside ", "over ", "past ", "since ", "through ", "throughout ", "to ",
            "toward ", "under ", "until ", "up ", "upon ", "without ", "with ", "within ",
        };
        static constexpr size_t kNumPrepositions = sizeof(Prepositions) / sizeof(Prepositions[0]);

        const char *Auxiliaries[] =
        {
            "do ", "may ", "might ", "shall ", "will ", "would ", "can ", "could ", "should ", "ought to ", "must ",
            "will have to ", "shall have to ", "could have to ", "should have to ", "must have to ", "need to ", "try to ",
        };
        static constexpr size_t kNumAuxiliaries = sizeof(Auxiliaries) / sizeof(Auxiliaries[0]);

        const char *Terminators[] =
        {
            ".", ";", ":", "?", "!", "--",
        };
        static constexpr size_t kNumTerminators = sizeof(Terminators) / sizeof(Terminators[0]);

        bool TpchText::GenerateWord(int64_t &offset, random::pcg32_fast &rng, char *arr, const char **words, size_t num_choices)
        {
            std::uniform_int_distribution<size_t> dist(0, num_choices - 1);
            const char *word = words[dist(rng)];
            size_t length = std::strlen(word);
            if(offset + length > kChunkSize)
                return false;
            std::memcpy(arr + offset, word, length);
            offset += length;
            return true;
        }

        bool TpchText::GenerateNoun(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            return GenerateWord(offset, rng, arr, Nouns, kNumNouns);
        }

        bool TpchText::GenerateVerb(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            return GenerateWord(offset, rng, arr, Verbs, kNumVerbs);
        }

        bool TpchText::GenerateAdjective(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            return GenerateWord(offset, rng, arr, Adjectives, kNumAdjectives);
        }

        bool TpchText::GenerateAdverb(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            return GenerateWord(offset, rng, arr, Adverbs, kNumAdverbs);
        }

        bool TpchText::GeneratePreposition(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            return GenerateWord(offset, rng, arr, Prepositions, kNumPrepositions);
        }

        bool TpchText::GenerateAuxiliary(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            return GenerateWord(offset, rng, arr, Auxiliaries, kNumAuxiliaries);
        }

        bool TpchText::GenerateTerminator(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            bool result = GenerateWord(offset, rng, arr, Terminators, kNumTerminators);
            // Swap the space with the terminator
            if(result)
                std::swap(*(arr + offset - 2), *(arr + offset - 1));
            return result;
        }

        bool TpchText::GenerateNounPhrase(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            std::uniform_int_distribution<size_t> dist(0, 3);
            const char *comma_space = ", ";
            bool success = true;
            switch(dist(rng))
            {
            case 0:
                success &= GenerateNoun(offset, rng, arr);
                break;
            case 1:
                success &= GenerateAdjective(offset, rng, arr);
                success &= GenerateNoun(offset, rng, arr);
                break;
            case 2:
                success &= GenerateAdjective(offset, rng, arr);
                success &= GenerateWord(--offset, rng, arr, &comma_space, 1);
                success &= GenerateAdjective(offset, rng, arr);
                success &= GenerateNoun(offset, rng, arr);
                break;
            case 3:
                GenerateAdverb(offset, rng, arr);
                GenerateAdjective(offset, rng, arr);
                GenerateNoun(offset, rng, arr);
                break;
            default:
                Unreachable("Random number should be between 0 and 3 inclusive");
                break;
            }
            return success;
        }

        bool TpchText::GenerateVerbPhrase(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            std::uniform_int_distribution<size_t> dist(0, 3);
            bool success = true;
            switch(dist(rng))
            {
            case 0:
                success &= GenerateVerb(offset, rng, arr);
                break;
            case 1:
                success &= GenerateAuxiliary(offset, rng, arr);
                success &= GenerateVerb(offset, rng, arr);
                break;
            case 2:
                success &= GenerateVerb(offset, rng, arr);
                success &= GenerateAdverb(offset, rng, arr);
                break;
            case 3:
                success &= GenerateAuxiliary(offset, rng, arr);
                success &= GenerateVerb(offset, rng, arr);
                success &= GenerateAdverb(offset, rng, arr);
                break;
            default:
                Unreachable("Random number should be between 0 and 3 inclusive");
                break;
            }
            return success;
        }

        bool TpchText::GeneratePrepositionalPhrase(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            const char *the_space = "the ";
            bool success = true;
            success &= GeneratePreposition(offset, rng, arr);
            success &= GenerateWord(offset, rng, arr, &the_space, 1);
            success &= GenerateNounPhrase(offset, rng, arr);
            return success;
        }

        bool TpchText::GenerateSentence(int64_t &offset, random::pcg32_fast &rng, char *arr)
        {
            std::uniform_int_distribution<size_t> dist(0, 4);
            bool success = true;
            switch(dist(rng))
            {
            case 0:
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateVerbPhrase(offset, rng, arr);
                success &= GenerateTerminator(offset, rng, arr);
                break;
            case 1:
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateVerbPhrase(offset, rng, arr);
                success &= GeneratePrepositionalPhrase(offset, rng, arr);
                success &= GenerateTerminator(offset, rng, arr);
                break;
            case 2:
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateVerbPhrase(offset, rng, arr);
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateTerminator(offset, rng, arr);
                break;
            case 3:
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateVerbPhrase(offset, rng, arr);
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateTerminator(offset, rng, arr);
                break;
            case 4:
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GeneratePrepositionalPhrase(offset, rng, arr);
                success &= GenerateVerbPhrase(offset, rng, arr);
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GenerateTerminator(offset, rng, arr);
                break;
            case 5:
                success &= GenerateNounPhrase(offset, rng, arr);
                success &= GeneratePrepositionalPhrase(offset, rng, arr);
                success &= GenerateVerbPhrase(offset, rng, arr);
                success &= GeneratePrepositionalPhrase(offset, rng, arr);
                success &= GenerateTerminator(offset, rng, arr);
                break;
            default:
                Unreachable("Random number should be between 0 and 5 inclusive");
                break;
            }
            return success;
        }

        using GenerateColumnFn = std::function<Status(size_t)>;
        class PartAndPartSupplierGenerator
        {
        public:
            Status Init(
                size_t num_threads,
                int64_t batch_size,
                float scale_factor)
            {
                if(!inited_)
                {
                    inited_ = true;
                    batch_size_ = batch_size;
                    scale_factor_ = scale_factor;

                    thread_local_data_.resize(num_threads);
                    for(ThreadLocalData &tld : thread_local_data_)
                    {
                        // 5 is the maximum number of different strings we need to concatenate
                        tld.string_indices.resize(5 * batch_size_);
                    }
                    part_rows_to_generate_ = static_cast<int64_t>(scale_factor_ * 200000);
                }
                return Status::OK();
            }
            
            int64_t part_batches_generated() const
            {
                return part_batches_generated_.load();
            }

            int64_t partsupp_batches_generated() const
            {
                return partsupp_batches_generated_.load();
            }

            Result<std::shared_ptr<Schema>> SetPartOutputColumns(const std::vector<std::string> &cols)
            {
                return SetOutputColumns(cols, part_types_, part_name_map_, part_cols_);
            }

            Result<std::shared_ptr<Schema>> SetPartSuppOutputColumns(const std::vector<std::string> &cols)
            {
                return SetOutputColumns(cols, partsupp_types_, partsupp_name_map_, partsupp_cols_);
            }

            Result<util::optional<ExecBatch>> NextPartBatch()
            {
                size_t thread_index = thread_indexer_();
                ThreadLocalData &tld = thread_local_data_[thread_index];
                {
                    std::lock_guard<std::mutex> lock(part_output_queue_mutex_);
                    bool all_generated = part_rows_generated_ == part_rows_to_generate_;
                    if(!part_output_queue_.empty())
                    {
                        ExecBatch batch = std::move(part_output_queue_.front());
                        part_output_queue_.pop();
                        return std::move(batch);
                    }
                    else if(all_generated)
                    {
                        return util::nullopt;
                    }
                    else
                    {
                        tld.partkey_start = part_rows_generated_;
                        tld.part_to_generate = std::min(
                            batch_size_,
                            part_rows_to_generate_ - part_rows_generated_);
                        part_rows_generated_ += tld.part_to_generate;

                        int64_t num_ps_batches = PartsuppBatchesToGenerate(thread_index);
                        part_batches_generated_.fetch_add(1);
                        partsupp_batches_generated_.fetch_add(num_ps_batches);
                        ARROW_DCHECK(part_rows_generated_ <= part_rows_to_generate_);
                    }
                }
                tld.part.resize(PART::kNumCols);
                std::fill(tld.part.begin(), tld.part.end(), Datum());
                RETURN_NOT_OK(InitPartsupp(thread_index));

                for(int col : part_cols_)
                {
                    RETURN_NOT_OK(part_generators_[col](thread_index));
                }
                for(int col : partsupp_cols_)
                    RETURN_NOT_OK(partsupp_generators_[col](thread_index));

                std::vector<Datum> part_result(part_cols_.size());
                for(size_t i = 0; i < part_cols_.size(); i++)
                {
                    int col_idx = part_cols_[i];
                    part_result[i] = tld.part[col_idx];
                }
                if(!partsupp_cols_.empty())
                {
                    std::vector<ExecBatch> partsupp_results;
                    for(size_t ibatch = 0; ibatch < tld.partsupp.size(); ibatch++)
                    {
                        std::vector<Datum> partsupp_result(partsupp_cols_.size());
                        for(size_t icol = 0; icol < partsupp_cols_.size(); icol++)
                        {
                            int col_idx = partsupp_cols_[icol];
                            partsupp_result[icol] = tld.partsupp[ibatch][col_idx];
                        }
                        ARROW_ASSIGN_OR_RAISE(ExecBatch eb, ExecBatch::Make(std::move(partsupp_result)));
                        partsupp_results.emplace_back(std::move(eb));
                    }
                    {
                        std::lock_guard<std::mutex> guard(partsupp_output_queue_mutex_);
                        for(ExecBatch &eb : partsupp_results)
                        {
                            partsupp_output_queue_.emplace(std::move(eb));
                        }
                    }
                }
                return ExecBatch::Make(std::move(part_result));
            }

            Result<util::optional<ExecBatch>> NextPartSuppBatch()
            {
                size_t thread_index = thread_indexer_();
                ThreadLocalData &tld = thread_local_data_[thread_index];
                {
                    std::lock_guard<std::mutex> lock(partsupp_output_queue_mutex_);
                    if(!partsupp_output_queue_.empty())
                    {
                        ExecBatch result = std::move(partsupp_output_queue_.front());
                        partsupp_output_queue_.pop();
                        return std::move(result);
                    }
                }
                {
                    std::lock_guard<std::mutex> lock(part_output_queue_mutex_);
                    if(part_rows_generated_ == part_rows_to_generate_)
                    {
                        return util::nullopt;
                    }
                    else
                    {
                        tld.partkey_start = part_rows_generated_;
                        tld.part_to_generate = std::min(
                            batch_size_,
                            part_rows_to_generate_ - part_rows_generated_);
                        part_rows_generated_ += tld.part_to_generate;
                        int64_t num_ps_batches = PartsuppBatchesToGenerate(thread_index);
                        part_batches_generated_.fetch_add(1);
                        partsupp_batches_generated_.fetch_add(num_ps_batches);
                        ARROW_DCHECK(part_rows_generated_ <= part_rows_to_generate_);
                    }
                }
                tld.part.resize(PART::kNumCols);
                std::fill(tld.part.begin(), tld.part.end(), Datum());
                RETURN_NOT_OK(InitPartsupp(thread_index));

                for(int col : part_cols_)
                    RETURN_NOT_OK(part_generators_[col](thread_index));
                for(int col : partsupp_cols_)
                    RETURN_NOT_OK(partsupp_generators_[col](thread_index));
                if(!part_cols_.empty())
                {
                    std::vector<Datum> part_result(part_cols_.size());
                    for(size_t i = 0; i < part_cols_.size(); i++)
                    {
                        int col_idx = part_cols_[i];
                        part_result[i] = tld.part[col_idx];
                    }
                    ARROW_ASSIGN_OR_RAISE(ExecBatch part_batch, ExecBatch::Make(std::move(part_result)));
                    {
                        std::lock_guard<std::mutex> lock(part_output_queue_mutex_);
                        part_output_queue_.emplace(std::move(part_batch));
                    }
                }
                std::vector<ExecBatch> partsupp_results;
                for(size_t ibatch = 0; ibatch < tld.partsupp.size(); ibatch++)
                {
                    std::vector<Datum> partsupp_result(partsupp_cols_.size());
                    for(size_t icol = 0; icol < partsupp_cols_.size(); icol++)
                    {
                        int col_idx = partsupp_cols_[icol];
                        partsupp_result[icol] = tld.partsupp[ibatch][col_idx];
                    }
                    ARROW_ASSIGN_OR_RAISE(ExecBatch eb, ExecBatch::Make(std::move(partsupp_result)));
                    partsupp_results.emplace_back(std::move(eb));
                }
                // Return the first batch, enqueue the rest.
                {
                    std::lock_guard<std::mutex> lock(partsupp_output_queue_mutex_);
                    for(size_t i = 1; i < partsupp_results.size(); i++)
                        partsupp_output_queue_.emplace(std::move(partsupp_results[i]));
                }
                return std::move(partsupp_results[0]);
            }

        private:
#define FOR_EACH_PART_COLUMN(F)                 \
            F(P_PARTKEY)                        \
            F(P_NAME)                           \
            F(P_MFGR)                           \
            F(P_BRAND)                          \
            F(P_TYPE)                           \
            F(P_SIZE)                           \
            F(P_CONTAINER)                      \
            F(P_RETAILPRICE)                    \
            F(P_COMMENT)

#define FOR_EACH_PARTSUPP_COLUMN(F)             \
            F(PS_PARTKEY)                       \
            F(PS_SUPPKEY)                       \
            F(PS_AVAILQTY)                      \
            F(PS_SUPPLYCOST)                    \
            F(PS_COMMENT)                       \

#define MAKE_ENUM(col) col,
            struct PART
            {
                enum
                {
                    FOR_EACH_PART_COLUMN(MAKE_ENUM)
                    kNumCols,
                };
            };
            struct PARTSUPP
            {
                enum
                {
                    FOR_EACH_PARTSUPP_COLUMN(MAKE_ENUM)
                    kNumCols,
                };
            };

#define MAKE_STRING_MAP(col)                            \
            { #col, PART::col },
            const std::unordered_map<std::string, int> part_name_map_ =
            {
                FOR_EACH_PART_COLUMN(MAKE_STRING_MAP)
            };
#undef MAKE_STRING_MAP
#define MAKE_STRING_MAP(col)                            \
            { #col, PARTSUPP::col },
            const std::unordered_map<std::string, int> partsupp_name_map_ =
            {
                FOR_EACH_PARTSUPP_COLUMN(MAKE_STRING_MAP)
            };
#undef MAKE_STRING_MAP
#define MAKE_FN_ARRAY(col)                                              \
            [this](size_t thread_index) { return this->col(thread_index); },
            std::vector<GenerateColumnFn> part_generators_ =
            {
                FOR_EACH_PART_COLUMN(MAKE_FN_ARRAY)
            };
            std::vector<GenerateColumnFn> partsupp_generators_ =
            {
                FOR_EACH_PARTSUPP_COLUMN(MAKE_FN_ARRAY)
            };
#undef MAKE_FN_ARRAY
#undef FOR_EACH_LINEITEM_COLUMN
#undef FOR_EACH_ORDERS_COLUMN

            const std::vector<std::shared_ptr<DataType>> part_types_ =
            {
                int32(),
                utf8(),
                fixed_size_binary(25),
                fixed_size_binary(10),
                utf8(),
                int32(),
                fixed_size_binary(10),
                decimal(12, 2),
                utf8(),
            };

            const std::vector<std::shared_ptr<DataType>> partsupp_types_ =
            {
                int32(),
                int32(),
                int32(),
                decimal(12, 2),
                utf8(),
            };

            Status AllocatePartBatch(size_t thread_index, int column)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                ARROW_DCHECK(tld.part[column].kind() == Datum::NONE);
                int32_t byte_width = arrow::internal::GetByteWidth(*part_types_[column]);
                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> buff, AllocateBuffer(tld.part_to_generate * byte_width));
                ArrayData ad(part_types_[column], tld.part_to_generate, { nullptr, std::move(buff) });
                tld.part[column] = std::move(ad);
                return Status::OK();
            }

            Status P_PARTKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_PARTKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocatePartBatch(thread_index, PART::P_PARTKEY));
                    int32_t *p_partkey = reinterpret_cast<int32_t *>(
                        tld.part[PART::P_PARTKEY].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.part_to_generate; i++)
                    {
                        p_partkey[i] = (tld.partkey_start + i + 1);
                        ARROW_DCHECK(1 <= p_partkey[i] && p_partkey[i] <= part_rows_to_generate_);
                    }
                }
                return Status::OK();
            }

            Status P_NAME(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_NAME].kind() == Datum::NONE)
                {
                    std::uniform_int_distribution<uint8_t> dist(0, static_cast<uint8_t>(kNumNameParts - 1));
                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> offset_buff, AllocateBuffer((tld.part_to_generate + 1) * sizeof(int32_t)));
                    int32_t *offsets = reinterpret_cast<int32_t *>(offset_buff->mutable_data());
                    offsets[0] = 0;
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        size_t string_length = 0;
                        for(int ipart = 0; ipart < 5; ipart++)
                        {
                            uint8_t name_part_index = dist(tld.rng);
                            tld.string_indices[irow * 5 + ipart] = name_part_index;
                            string_length += std::strlen(NameParts[name_part_index]);
                        }
                        // Add 4 because there is a space between each word (i.e. four spaces)
                        offsets[irow + 1] = offsets[irow] + string_length + 4;
                    }
                    // Add an extra byte for the space after in the very last string.
                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> string_buffer, AllocateBuffer(offsets[tld.part_to_generate] + 1));
                    char *strings = reinterpret_cast<char *>(string_buffer->mutable_data());
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        char *row = strings + offsets[irow];
                        for(int ipart = 0; ipart < 5; ipart++)
                        {
                            uint8_t name_part_index = tld.string_indices[irow * 5 + ipart];
                            const char *part = NameParts[name_part_index];
                            size_t length = std::strlen(part);
                            std::memcpy(row, part, length);
                            row += length;
                            *row++ = ' ';
                        }
                    }
                    ArrayData ad(part_types_[PART::P_NAME], tld.part_to_generate, { nullptr, std::move(offset_buff), std::move(string_buffer) });
                    Datum datum(ad);
                    tld.part[PART::P_NAME] = std::move(datum);
                }
                return Status::OK();
            }

            Status P_MFGR(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_MFGR].kind() == Datum::NONE)
                {
                    std::uniform_int_distribution<int> dist(1, 5);
                    const char *manufacturer = "Manufacturer#";
                    const size_t manufacturer_length = std::strlen(manufacturer);
                    RETURN_NOT_OK(AllocatePartBatch(thread_index, PART::P_MFGR));
                    char *p_mfgr = reinterpret_cast<char *>(tld.part[PART::P_MFGR].array()->buffers[1]->mutable_data());
                    int32_t byte_width = arrow::internal::GetByteWidth(*part_types_[PART::P_MFGR]);
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        std::strncpy(p_mfgr + byte_width * irow, manufacturer, byte_width);
                        char mfgr_id = '0' + dist(tld.rng);
                        *(p_mfgr + byte_width * irow + manufacturer_length) = mfgr_id;
                    }
                }
                return Status::OK();
            }

            Status P_BRAND(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_BRAND].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(P_MFGR(thread_index));
                    std::uniform_int_distribution<int> dist(1, 5);
                    const char *brand = "Brand#";
                    const size_t brand_length = std::strlen(brand);
                    RETURN_NOT_OK(AllocatePartBatch(thread_index, PART::P_BRAND));
                    const char *p_mfgr = reinterpret_cast<const char *>(
                        tld.part[PART::P_MFGR].array()->buffers[1]->data());
                    char *p_brand = reinterpret_cast<char *>(
                        tld.part[PART::P_BRAND].array()->buffers[1]->mutable_data());
                    int32_t byte_width = arrow::internal::GetByteWidth(*part_types_[PART::P_BRAND]);
                    int32_t mfgr_byte_width = arrow::internal::GetByteWidth(*part_types_[PART::P_MFGR]);
                    const size_t mfgr_id_offset = std::strlen("Manufacturer#");
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        char *row = p_brand + byte_width * irow;
                        char mfgr_id = *(p_mfgr + irow * mfgr_byte_width + mfgr_id_offset);
                        char brand_id = '0' + dist(tld.rng);
                        std::strncpy(row, brand, byte_width);
                        *(row + brand_length) = mfgr_id;
                        *(row + brand_length + 1) = brand_id;
                        irow += 0;
                    }
                }
                return Status::OK();
            }

            Status P_TYPE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_TYPE].kind() == Datum::NONE)
                {
                    using D = std::uniform_int_distribution<uint8_t>;
                    D dists[] =
                    {
                        D{ 0, static_cast<uint8_t>(kNumTypes_1 - 1) },
                        D{ 0, static_cast<uint8_t>(kNumTypes_2 - 1) },
                        D{ 0, static_cast<uint8_t>(kNumTypes_3 - 1) },
                    };

                    const char **types[] = { Types_1, Types_2, Types_3 };

                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> offset_buff, AllocateBuffer((tld.part_to_generate + 1) * sizeof(int32_t)));
                    int32_t *offsets = reinterpret_cast<int32_t *>(offset_buff->mutable_data());
                    offsets[0] = 0;
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        size_t string_length = 0;
                        for(int ipart = 0; ipart < 3; ipart++)
                        {
                            uint8_t name_part_index = dists[ipart](tld.rng);
                            tld.string_indices[irow * 3 + ipart] = name_part_index;
                            string_length += std::strlen(types[ipart][name_part_index]);
                        }
                        offsets[irow + 1] = offsets[irow] + string_length;
                    }
                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> string_buffer, AllocateBuffer(offsets[tld.part_to_generate]));
                    char *strings = reinterpret_cast<char *>(string_buffer->mutable_data());
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        char *row = strings + offsets[irow];
                        for(int ipart = 0; ipart < 3; ipart++)
                        {
                            uint8_t name_part_index = tld.string_indices[irow * 3 + ipart];
                            const char *part = types[ipart][name_part_index];
                            size_t length = std::strlen(part);
                            std::memcpy(row, part, length);
                            row += length;
                        }
                    }
                    ArrayData ad(part_types_[PART::P_TYPE], tld.part_to_generate, { nullptr, std::move(offset_buff), std::move(string_buffer) });
                    Datum datum(ad);
                    tld.part[PART::P_TYPE] = std::move(datum);
                }
                return Status::OK();
            }

            Status P_SIZE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_SIZE].kind() == Datum::NONE)
                {
                    std::uniform_int_distribution<int32_t> dist(1, 50);
                    RETURN_NOT_OK(AllocatePartBatch(thread_index, PART::P_SIZE));
                    int32_t *p_size = reinterpret_cast<int32_t *>(
                        tld.part[PART::P_SIZE].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.part_to_generate; i++)
                        p_size[i] = dist(tld.rng);
                }
                return Status::OK();
            }

            Status P_CONTAINER(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_CONTAINER].kind() == Datum::NONE)
                {
                    std::uniform_int_distribution<int> dist1(0, static_cast<uint8_t>(kNumContainers_1 - 1));
                    std::uniform_int_distribution<int> dist2(0, static_cast<uint8_t>(kNumContainers_2 - 1));
                    RETURN_NOT_OK(AllocatePartBatch(thread_index, PART::P_CONTAINER));
                    char *p_container = reinterpret_cast<char *>(
                        tld.part[PART::P_CONTAINER].array()->buffers[1]->mutable_data());
                    int32_t byte_width = arrow::internal::GetByteWidth(*part_types_[PART::P_CONTAINER]);
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        int container1_idx = dist1(tld.rng);
                        int container2_idx = dist2(tld.rng);
                        const char *container1 = Containers_1[container1_idx];
                        const char *container2 = Containers_2[container2_idx];
                        size_t container1_length = std::strlen(container1);
                        size_t container2_length = std::strlen(container2);

                        char *row = p_container + byte_width * irow;
                        std::strncpy(row, container1, byte_width);
                        std::memcpy(row + container1_length, container2, container2_length);
                    }
                }
                return Status::OK();
            }

            Status P_RETAILPRICE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_RETAILPRICE].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(P_PARTKEY(thread_index));
                    RETURN_NOT_OK(AllocatePartBatch(thread_index, PART::P_RETAILPRICE));
                    const int32_t *p_partkey = reinterpret_cast<const int32_t *>(
                        tld.part[PART::P_PARTKEY].array()->buffers[1]->data());
                    Decimal128 *p_retailprice = reinterpret_cast<Decimal128 *>(
                        tld.part[PART::P_RETAILPRICE].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.part_to_generate; irow++)
                    {
                        int32_t partkey = p_partkey[irow];
                        int64_t retail_price = (90000 + ((partkey / 10) % 20001) + 100 * (partkey % 1000));
                        p_retailprice[irow] = { retail_price };
                    }
                }
                return Status::OK();
            }

            Status P_COMMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PART::P_COMMENT].kind() == Datum::NONE)
                {
                    ARROW_ASSIGN_OR_RAISE(tld.part[PART::P_COMMENT], g_text.GenerateComments(tld.part_to_generate, 5, 22, tld.rng));
                }
                return Status::OK();
            }
            
            int64_t PartsuppBatchesToGenerate(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                int64_t ps_to_generate = kPartSuppRowsPerPart * tld.part_to_generate;
                int64_t num_batches = (ps_to_generate + batch_size_ - 1) / batch_size_;
                return num_batches;
            }

            Status InitPartsupp(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                tld.generated_partsupp.reset();
                int64_t num_batches = PartsuppBatchesToGenerate(thread_index);
                tld.partsupp.resize(num_batches);
                for(std::vector<Datum> &batch : tld.partsupp)
                {
                    batch.resize(PARTSUPP::kNumCols);
                    std::fill(batch.begin(), batch.end(), Datum());
                }
                return Status::OK();
            }

            Status AllocatePartSuppBatch(size_t thread_index, size_t ibatch, int column)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                int32_t byte_width = arrow::internal::GetByteWidth(*partsupp_types_[column]);
                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> buff, AllocateBuffer(batch_size_ * byte_width));
                ArrayData ad(partsupp_types_[column], batch_size_, { nullptr, std::move(buff) });
                tld.partsupp[ibatch][column] = std::move(ad);
                return Status::OK();
            }

            Status PS_PARTKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_partsupp[PARTSUPP::PS_PARTKEY])
                {
                    tld.generated_partsupp[PARTSUPP::PS_PARTKEY] = true;
                    RETURN_NOT_OK(P_PARTKEY(thread_index));
                    const int32_t *p_partkey = reinterpret_cast<const int32_t *>(
                        tld.part[PART::P_PARTKEY].array()->buffers[1]->data());

                    size_t ibatch = 0;
                    int64_t ipartsupp = 0;
                    int64_t ipart = 0;
                    int64_t ps_to_generate = kPartSuppRowsPerPart * tld.part_to_generate;
                    for(int64_t irow = 0; irow < ps_to_generate; ibatch++)
                    {
                        RETURN_NOT_OK(AllocatePartSuppBatch(thread_index, ibatch, PARTSUPP::PS_PARTKEY));
                        int32_t *ps_partkey = reinterpret_cast<int32_t *>(
                            tld.partsupp[ibatch][PARTSUPP::PS_PARTKEY].array()->buffers[1]->mutable_data());
                        int64_t next_run = std::min(batch_size_, ps_to_generate - irow);

                        int64_t batch_offset = 0;
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; ipartsupp < kPartSuppRowsPerPart && irun < next_run; ipartsupp++, irun++)
                                ps_partkey[batch_offset++] = p_partkey[ipart];
                            if(ipartsupp == kPartSuppRowsPerPart)
                            {
                                ipartsupp = 0;
                                ipart++;
                            }
                        }
                        irow += next_run;
                        tld.partsupp[ibatch][PARTSUPP::PS_PARTKEY].array()->length = batch_offset;
                    }
                }
                return Status::OK();
            }

            Status PS_SUPPKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_partsupp[PARTSUPP::PS_SUPPKEY])
                {
                    tld.generated_partsupp[PARTSUPP::PS_SUPPKEY] = true;
                    RETURN_NOT_OK(P_PARTKEY(thread_index));
                    const int32_t *p_partkey = reinterpret_cast<const int32_t *>(
                        tld.part[PART::P_PARTKEY].array()->buffers[1]->data());

                    size_t ibatch = 0;
                    int64_t ipartsupp = 0;
                    int64_t ipart = 0;
                    int64_t ps_to_generate = kPartSuppRowsPerPart * tld.part_to_generate;
                    const int32_t S = static_cast<int32_t>(scale_factor_ * 10000);
                    for(int64_t irow = 0; irow < ps_to_generate; ibatch++)
                    {
                        RETURN_NOT_OK(AllocatePartSuppBatch(thread_index, ibatch, PARTSUPP::PS_SUPPKEY));
                        int32_t *ps_suppkey = reinterpret_cast<int32_t *>(
                            tld.partsupp[ibatch][PARTSUPP::PS_PARTKEY].array()->buffers[1]->mutable_data());
                        int64_t next_run = std::min(batch_size_, ps_to_generate - irow);

                        int64_t batch_offset = 0;
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; ipartsupp < kPartSuppRowsPerPart && irun < next_run; ipartsupp++, irun++)
                            {
                                int32_t supplier = static_cast<int32_t>(ipartsupp);
                                int32_t partkey = p_partkey[ipart];
                                ps_suppkey[batch_offset++] = (partkey + (supplier * ((S / 4) + (partkey - 1) / S))) % S + 1; 
                            }
                            if(ipartsupp == kPartSuppRowsPerPart)
                            {
                                ipartsupp = 0;
                                ipart++;
                            }
                        }
                        irow += next_run;
                        tld.partsupp[ibatch][PARTSUPP::PS_SUPPKEY].array()->length = batch_offset;
                    }
                }
                return Status::OK();
            }

            Status PS_AVAILQTY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_partsupp[PARTSUPP::PS_AVAILQTY])
                {
                    tld.generated_partsupp[PARTSUPP::PS_AVAILQTY] = true;
                    std::uniform_int_distribution<int32_t> dist(1, 9999);
                    int64_t ps_to_generate = kPartSuppRowsPerPart * tld.part_to_generate;
                    int64_t ibatch = 0;
                    for(int64_t irow = 0; irow < ps_to_generate; ibatch++)
                    {
                        RETURN_NOT_OK(AllocatePartSuppBatch(thread_index, ibatch, PARTSUPP::PS_AVAILQTY));
                        int32_t *ps_availqty = reinterpret_cast<int32_t *>(
                            tld.partsupp[ibatch][PARTSUPP::PS_AVAILQTY].array()->buffers[1]->mutable_data());
                        int64_t next_run = std::min(batch_size_, ps_to_generate - irow);
                        for(int64_t irun = 0; irun < next_run; irun++)
                            ps_availqty[irun] = dist(tld.rng);

                        tld.partsupp[ibatch][PARTSUPP::PS_AVAILQTY].array()->length = next_run;
                        irow += next_run;
                    }
                }
                return Status::OK();
            }

            Status PS_SUPPLYCOST(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_partsupp[PARTSUPP::PS_SUPPLYCOST])
                {
                    tld.generated_partsupp[PARTSUPP::PS_SUPPLYCOST] = true;
                    std::uniform_int_distribution<int64_t> dist(100, 100000);
                    int64_t ps_to_generate = kPartSuppRowsPerPart * tld.part_to_generate;
                    int64_t ibatch = 0;
                    for(int64_t irow = 0; irow < ps_to_generate; ibatch++)
                    {
                        RETURN_NOT_OK(AllocatePartSuppBatch(thread_index, ibatch, PARTSUPP::PS_SUPPLYCOST));
                        Decimal128 *ps_supplycost = reinterpret_cast<Decimal128 *>(
                            tld.partsupp[ibatch][PARTSUPP::PS_SUPPLYCOST].array()->buffers[1]->mutable_data());
                        int64_t next_run = std::min(batch_size_, ps_to_generate - irow);
                        for(int64_t irun = 0; irun < next_run; irun++)
                            ps_supplycost[irun] = { dist(tld.rng) };

                        tld.partsupp[ibatch][PARTSUPP::PS_SUPPLYCOST].array()->length = next_run;
                        irow += next_run;
                    }
                }
                return Status::OK();
            }

            Status PS_COMMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.part[PARTSUPP::PS_COMMENT].kind() == Datum::NONE)
                {
                    int64_t irow = 0;
                    int64_t ps_to_generate = kPartSuppRowsPerPart * tld.part_to_generate;
                    for(size_t ibatch = 0; ibatch < tld.partsupp.size(); ibatch++)
                    {
                        int64_t num_rows = std::min(batch_size_, ps_to_generate - irow);
                        ARROW_ASSIGN_OR_RAISE(
                            tld.partsupp[ibatch][PARTSUPP::PS_COMMENT], g_text.GenerateComments(num_rows, 49, 198, tld.rng));
                        irow += num_rows;
                    }
                }
                return Status::OK();
            }

            struct ThreadLocalData
            {
                std::vector<Datum> part;
                std::vector<int8_t> string_indices;
                int64_t part_to_generate;
                int64_t partkey_start;

                std::vector<std::vector<Datum>> partsupp;
                std::bitset<PARTSUPP::kNumCols> generated_partsupp;
                random::pcg32_fast rng;
            };
            std::vector<ThreadLocalData> thread_local_data_;

            bool inited_ = false;
            std::mutex part_output_queue_mutex_;
            std::mutex partsupp_output_queue_mutex_;
            std::queue<ExecBatch> part_output_queue_;
            std::queue<ExecBatch> partsupp_output_queue_;
            int64_t batch_size_;
            float scale_factor_;
            int64_t part_rows_to_generate_;
            int64_t part_rows_generated_;
            std::vector<int> part_cols_;
            std::vector<int> partsupp_cols_;
            ThreadIndexer thread_indexer_;

            std::atomic<size_t> part_batches_generated_ = { 0 };
            std::atomic<size_t> partsupp_batches_generated_ = { 0 };
            static constexpr int64_t kPartSuppRowsPerPart = 4;
        };

        class OrdersAndLineItemGenerator
        {
        public:
            Status Init(
                size_t num_threads,
                int64_t batch_size,
                float scale_factor)
            {
                if(!inited_)
                {
                    inited_ = true;
                    batch_size_ = batch_size;
                    scale_factor_ = scale_factor;

                    thread_local_data_.resize(num_threads);
                    for(ThreadLocalData &tld : thread_local_data_)
                    {
                        tld.items_per_order.resize(batch_size_);
                    }
                    orders_rows_to_generate_ = static_cast<int64_t>(scale_factor_ * 150000 * 10);
                }
                return Status::OK();
            }

            int64_t orders_batches_generated() const
            {
                return orders_batches_generated_.load();
            }

            int64_t lineitem_batches_generated() const
            {
                return lineitem_batches_generated_.load();
            }

            Result<std::shared_ptr<Schema>> SetOrdersOutputColumns(const std::vector<std::string> &cols)
            {
                return SetOutputColumns(cols, orders_types_, orders_name_map_, orders_cols_);
            }

            Result<std::shared_ptr<Schema>> SetLineItemOutputColumns(const std::vector<std::string> &cols)
            {
                return SetOutputColumns(cols, lineitem_types_, lineitem_name_map_, lineitem_cols_);
            }

            Result<util::optional<ExecBatch>> NextOrdersBatch()
            {
                size_t thread_index = thread_indexer_();
                ThreadLocalData &tld = thread_local_data_[thread_index];
                {
                    std::lock_guard<std::mutex> lock(orders_output_queue_mutex_);
                    if(!orders_output_queue_.empty())
                    {
                        ExecBatch batch = std::move(orders_output_queue_.front());
                        orders_output_queue_.pop();
                        return std::move(batch);
                    }
                    else if(orders_rows_generated_ == orders_rows_to_generate_)
                    {
                        return util::nullopt;
                    }
                    else
                    {
                        tld.orderkey_start = orders_rows_generated_;
                        tld.orders_to_generate = std::min(
                            batch_size_,
                            orders_rows_to_generate_ - orders_rows_generated_);
                        orders_rows_generated_ += tld.orders_to_generate;
                        orders_batches_generated_.fetch_add(1);
                        ARROW_DCHECK(orders_rows_generated_ <= orders_rows_to_generate_);
                    }
                }
                tld.orders.resize(ORDERS::kNumCols);
                std::fill(tld.orders.begin(), tld.orders.end(), Datum());
                RETURN_NOT_OK(GenerateRowCounts(thread_index));
                tld.first_batch_offset = 0;
                tld.generated_lineitem.reset();

                for(int col : orders_cols_)
                    RETURN_NOT_OK(orders_generators_[col](thread_index));
                for(int col : lineitem_cols_)
                    RETURN_NOT_OK(lineitem_generators_[col](thread_index));

                std::vector<Datum> orders_result(orders_cols_.size());
                for(size_t i = 0; i < orders_cols_.size(); i++)
                {
                    int col_idx = orders_cols_[i];
                    orders_result[i] = tld.orders[col_idx];
                }
                if(!lineitem_cols_.empty())
                {
                    std::vector<ExecBatch> lineitem_results;
                    for(size_t ibatch = 0; ibatch < tld.lineitem.size(); ibatch++)
                    {
                        std::vector<Datum> lineitem_result(lineitem_cols_.size());
                        for(size_t icol = 0; icol < lineitem_cols_.size(); icol++)
                        {
                            int col_idx = lineitem_cols_[icol];
                            lineitem_result[icol] = tld.lineitem[ibatch][col_idx];
                        }
                        ARROW_ASSIGN_OR_RAISE(ExecBatch eb, ExecBatch::Make(std::move(lineitem_result)));
                        lineitem_results.emplace_back(std::move(eb));
                    }
                    {
                        std::lock_guard<std::mutex> guard(lineitem_output_queue_mutex_);
                        for(ExecBatch &eb : lineitem_results)
                        {
                            lineitem_output_queue_.emplace(std::move(eb));
                        }
                    }
                }
                return ExecBatch::Make(std::move(orders_result));
            }

            Result<util::optional<ExecBatch>> NextLineItemBatch()
            {
                size_t thread_index = thread_indexer_();
                ThreadLocalData &tld = thread_local_data_[thread_index];
                ExecBatch queued;
                bool from_queue = false;
                {
                    std::lock_guard<std::mutex> lock(lineitem_output_queue_mutex_);
                    if(!lineitem_output_queue_.empty())
                    {
                        queued = std::move(lineitem_output_queue_.front());
                        lineitem_output_queue_.pop();
                        from_queue = true;
                    }
                }
                tld.first_batch_offset = 0;
                if(from_queue)
                {
                    ARROW_DCHECK(queued.length <= batch_size_);
                    tld.first_batch_offset = queued.length;
                    if(queued.length == batch_size_)
                        return std::move(queued);
                }
                {
                    std::lock_guard<std::mutex> lock(orders_output_queue_mutex_);
                    if(orders_rows_generated_ == orders_rows_to_generate_)
                    {
                        if(from_queue)
                            return std::move(queued);
                        return util::nullopt;
                    }

                    tld.orderkey_start = orders_rows_generated_;
                    tld.orders_to_generate = std::min(
                        batch_size_,
                        orders_rows_to_generate_ - orders_rows_generated_);
                    orders_rows_generated_ += tld.orders_to_generate;
                    orders_batches_generated_.fetch_add(1ll);
                    ARROW_DCHECK(orders_rows_generated_ <= orders_rows_to_generate_);
                }
                tld.orders.resize(ORDERS::kNumCols);
                std::fill(tld.orders.begin(), tld.orders.end(), Datum());
                RETURN_NOT_OK(GenerateRowCounts(thread_index));
                tld.generated_lineitem.reset();
                if(from_queue)
                {
                    lineitem_batches_generated_.fetch_sub(1);
                    for(size_t i = 0; i < lineitem_cols_.size(); i++)
                        if(tld.lineitem[0][lineitem_cols_[i]].kind() == Datum::NONE)
                            tld.lineitem[0][lineitem_cols_[i]] = std::move(queued[i]);
                }

                for(int col : orders_cols_)
                    RETURN_NOT_OK(orders_generators_[col](thread_index));
                for(int col : lineitem_cols_)
                    RETURN_NOT_OK(lineitem_generators_[col](thread_index));

                if(!orders_cols_.empty())
                {
                    std::vector<Datum> orders_result(orders_cols_.size());
                    for(size_t i = 0; i < orders_cols_.size(); i++)
                    {
                        int col_idx = orders_cols_[i];
                        orders_result[i] = tld.orders[col_idx];
                    }
                    ARROW_ASSIGN_OR_RAISE(ExecBatch orders_batch, ExecBatch::Make(std::move(orders_result)));
                    {
                        std::lock_guard<std::mutex> lock(orders_output_queue_mutex_);
                        orders_output_queue_.emplace(std::move(orders_batch));
                    }
                }
                std::vector<ExecBatch> lineitem_results;
                for(size_t ibatch = 0; ibatch < tld.lineitem.size(); ibatch++)
                {
                    std::vector<Datum> lineitem_result(lineitem_cols_.size());
                    for(size_t icol = 0; icol < lineitem_cols_.size(); icol++)
                    {
                        int col_idx = lineitem_cols_[icol];
                        lineitem_result[icol] = tld.lineitem[ibatch][col_idx];
                    }
                    ARROW_ASSIGN_OR_RAISE(ExecBatch eb, ExecBatch::Make(std::move(lineitem_result)));
                    lineitem_results.emplace_back(std::move(eb));
                }
                lineitem_batches_generated_.fetch_add(static_cast<int64_t>(lineitem_results.size()));
                // Return the first batch, enqueue the rest.
                {
                    std::lock_guard<std::mutex> lock(lineitem_output_queue_mutex_);
                    for(size_t i = 1; i < lineitem_results.size(); i++)
                        lineitem_output_queue_.emplace(std::move(lineitem_results[i]));
                }
                return std::move(lineitem_results[0]);
            }

        private:
#define FOR_EACH_ORDERS_COLUMN(F)               \
            F(O_ORDERKEY)                       \
            F(O_CUSTKEY)                        \
            F(O_ORDERSTATUS)                    \
            F(O_TOTALPRICE)                     \
            F(O_ORDERDATE)                      \
            F(O_ORDERPRIORITY)                  \
            F(O_CLERK)                          \
            F(O_SHIPPRIORITY)                   \
            F(O_COMMENT)

#define FOR_EACH_LINEITEM_COLUMN(F)             \
            F(L_ORDERKEY)                       \
            F(L_PARTKEY)                        \
            F(L_SUPPKEY)                        \
            F(L_LINENUMBER)                     \
            F(L_QUANTITY)                       \
            F(L_EXTENDEDPRICE)                  \
            F(L_DISCOUNT)                       \
            F(L_TAX)                            \
            F(L_RETURNFLAG)                     \
            F(L_LINESTATUS)                     \
            F(L_SHIPDATE)                       \
            F(L_COMMITDATE)                     \
            F(L_RECEIPTDATE)                    \
            F(L_SHIPINSTRUCT)                   \
            F(L_SHIPMODE)                       \
            F(L_COMMENT)

#define MAKE_ENUM(col) col,
            struct ORDERS
            {
                enum
                {
                    FOR_EACH_ORDERS_COLUMN(MAKE_ENUM)
                    kNumCols,
                };
            };
            struct LINEITEM
            {
                enum
                {
                    FOR_EACH_LINEITEM_COLUMN(MAKE_ENUM)
                    kNumCols,
                };
            };

#define MAKE_STRING_MAP(col)                            \
            { #col, ORDERS::col },
            const std::unordered_map<std::string, int> orders_name_map_ =
            {
                FOR_EACH_ORDERS_COLUMN(MAKE_STRING_MAP)
            };
#undef MAKE_STRING_MAP
#define MAKE_STRING_MAP(col)                            \
            { #col, LINEITEM::col },
            const std::unordered_map<std::string, int> lineitem_name_map_ =
            {
                FOR_EACH_LINEITEM_COLUMN(MAKE_STRING_MAP)
            };
#undef MAKE_STRING_MAP
#define MAKE_FN_ARRAY(col)                                              \
            [this](size_t thread_index) { return this->col(thread_index); },
            std::vector<GenerateColumnFn> orders_generators_ =
            {
                FOR_EACH_ORDERS_COLUMN(MAKE_FN_ARRAY)
            };
            std::vector<GenerateColumnFn> lineitem_generators_ =
            {
                FOR_EACH_LINEITEM_COLUMN(MAKE_FN_ARRAY)
            };
#undef MAKE_FN_ARRAY
#undef FOR_EACH_LINEITEM_COLUMN
#undef FOR_EACH_ORDERS_COLUMN

            const std::vector<std::shared_ptr<DataType>> orders_types_ =
            {
                int32(),
                int32(),
                fixed_size_binary(1),
                decimal(12, 2),
                date32(),
                fixed_size_binary(15),
                fixed_size_binary(15),
                int32(),
                utf8()
            };

            const std::vector<std::shared_ptr<DataType>> lineitem_types_ =
            {
                int32(),
                int32(),
                int32(),
                int32(),
                decimal(12, 2),
                decimal(12, 2),
                decimal(12, 2),
                decimal(12, 2),
                fixed_size_binary(1),
                fixed_size_binary(1),
                date32(),
                date32(),
                date32(),
                fixed_size_binary(25),
                fixed_size_binary(10),
                utf8(),
            };

            Status AllocateOrdersBatch(size_t thread_index, int column)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                ARROW_DCHECK(tld.orders[column].kind() == Datum::NONE);
                int32_t byte_width = arrow::internal::GetByteWidth(*orders_types_[column]);
                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> buff, AllocateBuffer(tld.orders_to_generate * byte_width));
                ArrayData ad(orders_types_[column], tld.orders_to_generate, { nullptr, std::move(buff) });
                tld.orders[column] = std::move(ad);
                return Status::OK();
            }

            Status O_ORDERKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_ORDERKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_ORDERKEY));
                    int32_t *o_orderkey = reinterpret_cast<int32_t *>(
                        tld.orders[ORDERS::O_ORDERKEY].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.orders_to_generate; i++)
                    {
                        int32_t orderkey_index = tld.orderkey_start + i;
                        int32_t index_of_run = orderkey_index / 8;
                        int32_t index_in_run = orderkey_index % 8;
                        o_orderkey[i] = (index_of_run * 32 + index_in_run + 1);
                        ARROW_DCHECK(1 <= o_orderkey[i] && o_orderkey[i] <= 4 * orders_rows_to_generate_);
                    }
                }
                return Status::OK();
            }

            Status O_CUSTKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_CUSTKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_CUSTKEY));

                    // Spec says it must be a random number between 1 and SF*150000 that is not
                    // divisible by 3. Rather than repeatedly generating numbers until we get to
                    // a non-divisible-by-3 number, we just generate a number between
                    // 0 and SF * 50000 - 1, multiply by 3, and then add either 1 or 2. 
                    int32_t sf_50k = static_cast<int32_t>(scale_factor_ * 50000);
                    std::uniform_int_distribution<int32_t> base_dist(0, sf_50k - 1);
                    std::uniform_int_distribution<int32_t> offset_dist(1, 2);
                    int32_t *o_custkey = reinterpret_cast<int32_t *>(
                        tld.orders[ORDERS::O_CUSTKEY].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.orders_to_generate; i++)
                        o_custkey[i] = 3 * base_dist(tld.rng) + offset_dist(tld.rng);
                }
                return Status::OK();
            }

            Status O_ORDERSTATUS(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_ORDERSTATUS].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(L_LINESTATUS(thread_index));
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_ORDERSTATUS));

                    char *o_orderstatus = reinterpret_cast<char *>(
                        tld.orders[ORDERS::O_ORDERSTATUS].array()->buffers[1]->mutable_data());

                    size_t batch_offset = tld.first_batch_offset;
                    size_t ibatch = 0;
                    size_t iorder = 0;
                    int32_t iline = 0;
                    bool all_f = true;
                    bool all_o = true;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        const char *l_linestatus = reinterpret_cast<const char *>(
                            tld.lineitem[ibatch][LINEITEM::L_LINESTATUS].array()->buffers[1]->data());

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; iline < tld.items_per_order[iorder] && irun < next_run; iline++, irun++, batch_offset++)
                            {
                                all_f &= l_linestatus[batch_offset] == 'F';
                                all_o &= l_linestatus[batch_offset] == 'O';
                            }
                            if(iline == tld.items_per_order[iorder])
                            {
                                iline = 0;
                                ARROW_DCHECK(!(all_f && all_o));
                                if(all_f)
                                    o_orderstatus[iorder] = 'F';
                                else if(all_o)
                                    o_orderstatus[iorder] = 'O';
                                else
                                    o_orderstatus[iorder] = 'P';
                                iorder++;
                            }
                        }
                        irow += next_run;
                        batch_offset = 0;
                    }
                }
                return Status::OK();
            }

            Status O_TOTALPRICE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_TOTALPRICE].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(L_EXTENDEDPRICE(thread_index));
                    RETURN_NOT_OK(L_TAX(thread_index));
                    RETURN_NOT_OK(L_DISCOUNT(thread_index));
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_TOTALPRICE));

                    size_t batch_offset = tld.first_batch_offset;
                    size_t ibatch = 0;
                    size_t iorder = 0;
                    int32_t iline = 0;
                    int64_t sum = 0;
                    Decimal128 *o_totalprice = reinterpret_cast<Decimal128 *>(
                        tld.orders[ORDERS::O_TOTALPRICE].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);

                        const Decimal128 *l_extendedprice = reinterpret_cast<const Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_EXTENDEDPRICE].array()->buffers[1]->data());
                        const Decimal128 *l_tax = reinterpret_cast<const Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_TAX].array()->buffers[1]->data());
                        const Decimal128 *l_discount = reinterpret_cast<const Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_DISCOUNT].array()->buffers[1]->data());

                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; iline < tld.items_per_order[iorder] && irun < next_run; iline++, irun++, batch_offset++)
                            {
                                int64_t eprice = static_cast<int64_t>(l_extendedprice[batch_offset]);
                                int64_t tax = static_cast<int64_t>(l_tax[batch_offset]);
                                int64_t discount = static_cast<int64_t>(l_discount[batch_offset]);
                                sum += (eprice * (100 + tax) * (100 - discount));
                            }
                            if(iline == tld.items_per_order[iorder])
                            {
                                sum /= 100 * 100;
                                o_totalprice[iorder] = { sum };
                                iline = 0;
                                iorder++;
                            }
                        }
                        irow += next_run;
                        batch_offset = 0;
                    }
                }
                return Status::OK();
            }

            Status O_ORDERDATE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_ORDERDATE].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_ORDERDATE));

                    std::uniform_int_distribution<uint32_t> dist(STARTDATE, ENDDATE - 151);
                    uint32_t *o_orderdate = reinterpret_cast<uint32_t *>(
                        tld.orders[ORDERS::O_ORDERDATE].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.orders_to_generate; i++)
                        o_orderdate[i] = dist(tld.rng);
                }
                return Status::OK();
            }

            Status O_ORDERPRIORITY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_ORDERPRIORITY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_ORDERPRIORITY));
                    int32_t byte_width = arrow::internal::GetByteWidth(*orders_types_[ORDERS::O_ORDERPRIORITY]);
                    std::uniform_int_distribution<int32_t> dist(0, kNumPriorities - 1);
                    char *o_orderpriority = reinterpret_cast<char *>(
                        tld.orders[ORDERS::O_ORDERPRIORITY].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.orders_to_generate; i++)
                    {
                        const char *str = Priorities[dist(tld.rng)];
                        std::strncpy(o_orderpriority + i * byte_width, str, byte_width);
                    }
                }
                return Status::OK();
            }

            Status O_CLERK(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_CLERK].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_CLERK));
                    int32_t byte_width = arrow::internal::GetByteWidth(*orders_types_[ORDERS::O_CLERK]);
                    int64_t max_clerk_id = static_cast<int64_t>(scale_factor_ * 1000);
                    std::uniform_int_distribution<int64_t> dist(1, max_clerk_id);
                    char *o_clerk = reinterpret_cast<char *>(
                        tld.orders[ORDERS::O_CLERK].array()->buffers[1]->mutable_data());
                    for(int64_t i = 0; i < tld.orders_to_generate; i++)
                    {
                        const char *clerk = "Clerk#";
                        const size_t clerk_length = std::strlen(clerk);
                        int64_t clerk_number = dist(tld.rng);
                        char *output = o_clerk + i * byte_width;
                        std::strncpy(output, clerk, byte_width);
                        AppendNumberPaddedToNineDigits(output + clerk_length, clerk_number);
                    }
                }
                return Status::OK();
            }

            Status O_SHIPPRIORITY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_SHIPPRIORITY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateOrdersBatch(thread_index, ORDERS::O_SHIPPRIORITY));
                    int32_t *o_shippriority = reinterpret_cast<int32_t *>(
                        tld.orders[ORDERS::O_SHIPPRIORITY].array()->buffers[1]->mutable_data());
                    std::memset(o_shippriority, 0, tld.orders_to_generate * sizeof(int32_t));
                }
                return Status::OK();
            }

            Status O_COMMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.orders[ORDERS::O_COMMENT].kind() == Datum::NONE)
                {
                    ARROW_ASSIGN_OR_RAISE(tld.orders[ORDERS::O_COMMENT], g_text.GenerateComments(tld.orders_to_generate, 19, 78, tld.rng));
                }
                return Status::OK();
            }

            Status GenerateRowCounts(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                std::uniform_int_distribution<int> length_dist(1, 7);
                tld.lineitem_to_generate = 0;
                tld.items_per_order.clear();
                for(int64_t i = 0; i < tld.orders_to_generate; i++)
                {
                    int64_t length = length_dist(tld.rng);
                    tld.items_per_order.push_back(length);
                    tld.lineitem_to_generate += length;
                }
                int64_t num_batches = (tld.first_batch_offset + tld.lineitem_to_generate + batch_size_ - 1) / batch_size_;
                tld.lineitem.resize(num_batches);
                for(std::vector<Datum> &batch : tld.lineitem)
                {
                    batch.resize(LINEITEM::kNumCols);
                    std::fill(batch.begin(), batch.end(), Datum());
                }
                return Status::OK();
            }

            Status AllocateLineItemBufferIfNeeded(size_t thread_index, size_t ibatch, int column, size_t &out_batch_offset)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.lineitem[ibatch][column].kind() == Datum::NONE)
                {
                    int32_t byte_width = arrow::internal::GetByteWidth(*lineitem_types_[column]);
                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> buff, AllocateBuffer(batch_size_ * byte_width));
                    ArrayData ad(lineitem_types_[column], batch_size_, { nullptr, std::move(buff) });
                    tld.lineitem[ibatch][column] = std::move(ad);
                    out_batch_offset = 0;
                }
                if(ibatch == 0)
                    out_batch_offset = tld.first_batch_offset;

                return Status::OK();
            }

            Status L_ORDERKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_ORDERKEY])
                {
                    tld.generated_lineitem[LINEITEM::L_ORDERKEY] = true;
                    RETURN_NOT_OK(O_ORDERKEY(thread_index));
                    const int32_t *o_orderkey = reinterpret_cast<const int32_t *>(
                        tld.orders[ORDERS::O_ORDERKEY].array()->buffers[1]->data());

                    size_t ibatch = 0;
                    size_t iorder = 0;
                    int32_t iline = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_ORDERKEY, batch_offset));
                        int32_t *l_linenumber = reinterpret_cast<int32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_ORDERKEY].array()->buffers[1]->mutable_data());
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; iline < tld.items_per_order[iorder] && irun < next_run; iline++, irun++)
                                l_linenumber[batch_offset++] = o_orderkey[iorder];
                            if(iline == tld.items_per_order[iorder])
                            {
                                iline = 0;
                                iorder++;
                            }
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_ORDERKEY].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_PARTKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_PARTKEY])
                {
                    tld.generated_lineitem[LINEITEM::L_PARTKEY] = true;

                    size_t ibatch = 0;
                    int32_t max_partkey = static_cast<int32_t>(scale_factor_ * 200000);
                    std::uniform_int_distribution<int32_t> dist(1, max_partkey);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_PARTKEY, batch_offset));
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        int32_t *l_partkey = reinterpret_cast<int32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_PARTKEY].array()->buffers[1]->mutable_data());
                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                            l_partkey[batch_offset] = dist(tld.rng);

                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_PARTKEY].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_SUPPKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_SUPPKEY])
                {
                    tld.generated_lineitem[LINEITEM::L_SUPPKEY] = true;
                    RETURN_NOT_OK(L_PARTKEY(thread_index));

                    size_t ibatch = 0;
                    std::uniform_int_distribution<int32_t> dist(0, 3);
                    const int32_t S = static_cast<int32_t>(scale_factor_ * 10000);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset = 0;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_SUPPKEY, batch_offset));
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        int32_t *l_suppkey = reinterpret_cast<int32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_SUPPKEY].array()->buffers[1]->mutable_data());
                        const int32_t *l_partkey = reinterpret_cast<const int32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_PARTKEY].array()->buffers[1]->data());
                        for(int64_t i = 0; i < next_run; i++)
                        {
                            int32_t supplier = dist(tld.rng);
                            int32_t partkey = l_partkey[batch_offset];
                            // Fun fact: the parentheses for this expression are unbalanced in the TPC-H spec.
                            l_suppkey[batch_offset++] = (partkey + (supplier * ((S / 4) + (partkey - 1) / S))) % S + 1; 
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_SUPPKEY].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_LINENUMBER(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_LINENUMBER])
                {
                    tld.generated_lineitem[LINEITEM::L_LINENUMBER] = true;
                    size_t ibatch = 0;
                    size_t iorder = 0;
                    int32_t iline = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_LINENUMBER, batch_offset));
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        int32_t *l_linenumber = reinterpret_cast<int32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_LINENUMBER].array()->buffers[1]->mutable_data());
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; iline < tld.items_per_order[iorder] && irun < next_run; iline++, irun++)
                            {
                                l_linenumber[batch_offset++] = (iline + 1);
                                ARROW_DCHECK(1 <= (iline + 1) && (iline + 1) <= 7);
                            }
                            if(iline == tld.items_per_order[iorder])
                            {
                                iline = 0;
                                iorder++;
                            }
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_LINENUMBER].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_QUANTITY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_QUANTITY])
                {
                    tld.generated_lineitem[LINEITEM::L_QUANTITY] = true;

                    size_t ibatch = 0;
                    std::uniform_int_distribution<int64_t> dist(1, 50);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_QUANTITY, batch_offset));
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        Decimal128 *l_quantity = reinterpret_cast<Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_QUANTITY].array()->buffers[1]->mutable_data());
                        for(int64_t i = 0; i < next_run; i++)
                        {
                            // Multiply by 100 because the type is decimal(12, 2), so the decimal goes after two digits
                            int64_t quantity = dist(tld.rng) * 100;
                            l_quantity[batch_offset++] = { quantity };
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_QUANTITY].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_EXTENDEDPRICE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_EXTENDEDPRICE])
                {
                    tld.generated_lineitem[LINEITEM::L_EXTENDEDPRICE] = true;
                    RETURN_NOT_OK(L_PARTKEY(thread_index));
                    RETURN_NOT_OK(L_QUANTITY(thread_index));
                    size_t ibatch = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_EXTENDEDPRICE, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        const int32_t *l_partkey = reinterpret_cast<const int32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_PARTKEY].array()->buffers[1]->data());
                        const Decimal128 *l_quantity = reinterpret_cast<const Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_QUANTITY].array()->buffers[1]->data());
                        Decimal128 *l_extendedprice = reinterpret_cast<Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_EXTENDEDPRICE].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                        {
                            int64_t partkey = static_cast<int64_t>(l_partkey[batch_offset]);
                            // Divide by 100 to recover the integer representation (not Decimal).
                            int64_t quantity = static_cast<int64_t>(l_quantity[batch_offset]) / 100;

                            // Spec says to divide by 100, but that happens automatically due to this being stored
                            // to two decimal points.
                            int64_t retail_price = (90000 + ((partkey / 10) % 20001) + 100 * (partkey % 1000));
                            int64_t extended_price = retail_price * quantity;
                            l_extendedprice[batch_offset] = { extended_price };
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_EXTENDEDPRICE].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_DISCOUNT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_DISCOUNT])
                {
                    tld.generated_lineitem[LINEITEM::L_DISCOUNT] = true;
                    size_t ibatch = 0;
                    std::uniform_int_distribution<int32_t> dist(0, 10);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_DISCOUNT, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        Decimal128 *l_discount = reinterpret_cast<Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_DISCOUNT].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                            l_discount[batch_offset] = { dist(tld.rng) };
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_DISCOUNT].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_TAX(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_TAX])
                {
                    tld.generated_lineitem[LINEITEM::L_TAX] = true;
                    size_t ibatch = 0;
                    std::uniform_int_distribution<int32_t> dist(0, 8);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_TAX, batch_offset));
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        Decimal128 *l_tax = reinterpret_cast<Decimal128 *>(
                            tld.lineitem[ibatch][LINEITEM::L_TAX].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                            l_tax[batch_offset] = { dist(tld.rng) };
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_TAX].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_RETURNFLAG(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_RETURNFLAG])
                {
                    tld.generated_lineitem[LINEITEM::L_RETURNFLAG] = true;
                    RETURN_NOT_OK(L_RECEIPTDATE(thread_index));
                    size_t ibatch = 0;
                    std::uniform_int_distribution<uint32_t> dist;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_RETURNFLAG, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        char *l_returnflag = reinterpret_cast<char *>(
                            tld.lineitem[ibatch][LINEITEM::L_RETURNFLAG].array()->buffers[1]->mutable_data());
                        const uint32_t *l_receiptdate = reinterpret_cast<const uint32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_RECEIPTDATE].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                        {
                            if(l_receiptdate[batch_offset] <= CURRENTDATE)
                            {
                                uint32_t r = dist(tld.rng);
                                l_returnflag[batch_offset] = (r % 2 == 1) ? 'R' : 'A';
                            }
                            else
                            {
                                l_returnflag[batch_offset] = 'N';
                            }
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_RETURNFLAG].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_LINESTATUS(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_LINESTATUS])
                {
                    tld.generated_lineitem[LINEITEM::L_LINESTATUS] = true;
                    RETURN_NOT_OK(L_SHIPDATE(thread_index));
                    size_t ibatch = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_LINESTATUS, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        char *l_linestatus = reinterpret_cast<char *>(
                            tld.lineitem[ibatch][LINEITEM::L_LINESTATUS].array()->buffers[1]->mutable_data());
                        const uint32_t *l_shipdate = reinterpret_cast<const uint32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_SHIPDATE].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                        {
                            if(l_shipdate[batch_offset] > CURRENTDATE)
                                l_linestatus[batch_offset] = 'O';
                            else
                                l_linestatus[batch_offset] = 'F';
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_LINESTATUS].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_SHIPDATE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_SHIPDATE])
                {
                    tld.generated_lineitem[LINEITEM::L_SHIPDATE] = true;
                    RETURN_NOT_OK(O_ORDERDATE(thread_index));
                    const int32_t *o_orderdate = reinterpret_cast<const int32_t *>(
                        tld.orders[ORDERS::O_ORDERDATE].array()->buffers[1]->data());
                    std::uniform_int_distribution<uint32_t> dist(1, 121);
                    size_t ibatch = 0;
                    size_t iorder = 0;
                    int32_t iline = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_SHIPDATE, batch_offset));
                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        uint32_t *l_shipdate = reinterpret_cast<uint32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_SHIPDATE].array()->buffers[1]->mutable_data());
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; iline < tld.items_per_order[iorder] && irun < next_run; iline++, irun++)
                                l_shipdate[batch_offset++] = o_orderdate[iorder] + dist(tld.rng);
                            if(iline == tld.items_per_order[iorder])
                            {
                                iline = 0;
                                iorder++;
                            }
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_SHIPDATE].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_COMMITDATE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_COMMITDATE])
                {
                    tld.generated_lineitem[LINEITEM::L_COMMITDATE] = true;
                    const int32_t *o_orderdate = reinterpret_cast<const int32_t *>(
                        tld.orders[ORDERS::O_ORDERDATE].array()->buffers[1]->data());
                    std::uniform_int_distribution<uint32_t> dist(30, 90);
                    size_t ibatch = 0;
                    size_t iorder = 0;
                    int32_t iline = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_COMMITDATE, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        uint32_t *l_commitdate = reinterpret_cast<uint32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_COMMITDATE].array()->buffers[1]->mutable_data());
                        for(int64_t irun = 0; irun < next_run;)
                        {
                            for(; iline < tld.items_per_order[iorder] && irun < next_run; iline++, irun++)
                                l_commitdate[batch_offset++] = o_orderdate[iorder] + dist(tld.rng);
                            if(iline == tld.items_per_order[iorder])
                            {
                                iline = 0;
                                iorder++;
                            }
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_COMMITDATE].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_RECEIPTDATE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_RECEIPTDATE])
                {
                    tld.generated_lineitem[LINEITEM::L_RECEIPTDATE] = true;
                    RETURN_NOT_OK(L_SHIPDATE(thread_index));
                    size_t ibatch = 0;
                    std::uniform_int_distribution<int32_t> dist(1, 30);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_RECEIPTDATE, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        uint32_t *l_receiptdate = reinterpret_cast<uint32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_RECEIPTDATE].array()->buffers[1]->mutable_data());
                        const uint32_t *l_shipdate = reinterpret_cast<const uint32_t *>(
                            tld.lineitem[ibatch][LINEITEM::L_SHIPDATE].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                            l_receiptdate[batch_offset] = l_shipdate[batch_offset] + dist(tld.rng);

                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_RECEIPTDATE].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_SHIPINSTRUCT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_SHIPINSTRUCT])
                {
                    tld.generated_lineitem[LINEITEM::L_SHIPINSTRUCT] = true;
                    int32_t byte_width = arrow::internal::GetByteWidth(*lineitem_types_[LINEITEM::L_SHIPINSTRUCT]);
                    size_t ibatch = 0;
                    std::uniform_int_distribution<size_t> dist(0, kNumInstructions - 1);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_SHIPINSTRUCT, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        char *l_shipinstruct = reinterpret_cast<char *>(
                            tld.lineitem[ibatch][LINEITEM::L_SHIPINSTRUCT].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                        {
                            const char *str = Instructions[dist(tld.rng)];
                            // Note that we don't have to memset the buffer to 0 because strncpy pads each string
                            // with 0's anyway
                            std::strncpy(l_shipinstruct + batch_offset * byte_width, str, byte_width);
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_SHIPINSTRUCT].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_SHIPMODE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_SHIPMODE])
                {
                    tld.generated_lineitem[LINEITEM::L_SHIPMODE] = true;
                    int32_t byte_width = arrow::internal::GetByteWidth(*lineitem_types_[LINEITEM::L_SHIPMODE]);
                    size_t ibatch = 0;
                    std::uniform_int_distribution<size_t> dist(0, kNumModes - 1);
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        size_t batch_offset;
                        RETURN_NOT_OK(AllocateLineItemBufferIfNeeded(thread_index, ibatch, LINEITEM::L_SHIPMODE, batch_offset));

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);
                        char *l_shipmode = reinterpret_cast<char *>(
                            tld.lineitem[ibatch][LINEITEM::L_SHIPMODE].array()->buffers[1]->mutable_data());

                        for(int64_t i = 0; i < next_run; i++, batch_offset++)
                        {
                            const char *str = Modes[dist(tld.rng)];
                            std::strncpy(l_shipmode + batch_offset * byte_width, str, byte_width);
                        }
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_SHIPMODE].array()->length = static_cast<int64_t>(batch_offset);
                    }
                }
                return Status::OK();
            }

            Status L_COMMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(!tld.generated_lineitem[LINEITEM::L_COMMENT])
                {
                    tld.generated_lineitem[LINEITEM::L_COMMENT] = true;

                    size_t batch_offset = tld.first_batch_offset;
                    size_t ibatch = 0;
                    for(int64_t irow = 0; irow < tld.lineitem_to_generate; ibatch++)
                    {
                        // Comments are kind of sneaky: we always generate the full batch and then just bump the length
                        if(tld.lineitem[ibatch][LINEITEM::L_COMMENT].kind() == Datum::NONE)
                        {
                            ARROW_ASSIGN_OR_RAISE(tld.lineitem[ibatch][LINEITEM::L_COMMENT], g_text.GenerateComments(batch_size_, 10, 43, tld.rng));
                            batch_offset = 0;
                        }

                        int64_t remaining_in_batch = static_cast<int64_t>(batch_size_ - batch_offset);
                        int64_t next_run = std::min(tld.lineitem_to_generate - irow, remaining_in_batch);

                        batch_offset += next_run;
                        irow += next_run;
                        tld.lineitem[ibatch][LINEITEM::L_COMMENT].array()->length = batch_offset;
                    }
                }
                return Status::OK();
            }

            struct ThreadLocalData
            {
                std::vector<Datum> orders;
                int64_t orders_to_generate;
                int64_t orderkey_start;

                std::vector<std::vector<Datum>> lineitem;
                std::vector<int> items_per_order;
                int64_t lineitem_to_generate;
                int64_t first_batch_offset;
                std::bitset<LINEITEM::kNumCols> generated_lineitem;
                random::pcg32_fast rng;
            };
            std::vector<ThreadLocalData> thread_local_data_;

            bool inited_ = false;
            std::mutex orders_output_queue_mutex_;
            std::mutex lineitem_output_queue_mutex_;
            std::queue<ExecBatch> orders_output_queue_;
            std::queue<ExecBatch> lineitem_output_queue_;
            int64_t batch_size_;
            float scale_factor_;
            int64_t orders_rows_to_generate_;
            int64_t orders_rows_generated_;
            std::vector<int> orders_cols_;
            std::vector<int> lineitem_cols_;
            ThreadIndexer thread_indexer_;

            std::atomic<size_t> orders_batches_generated_ = { 0 };
            std::atomic<size_t> lineitem_batches_generated_ = { 0 };
        };

        class SupplierGenerator : public TpchTableGenerator
        {
        public:
            Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) override
            {
                scale_factor_ = scale_factor;
                batch_size_ = batch_size;
                rows_to_generate_ = static_cast<int64_t>(scale_factor_ * 10000);
                rows_generated_.store(0);
                ARROW_ASSIGN_OR_RAISE(schema_, SetOutputColumns(
                                          columns,
                                          types_,
                                          name_map_,
                                          gen_list_));

                random::pcg32_fast rng;
                std::uniform_int_distribution<int64_t> dist(0, rows_to_generate_ - 1);
                size_t num_special_rows = static_cast<size_t>(5 * scale_factor_);
                std::unordered_set<int64_t> good_rows_set;
                while(good_rows_set.size() < num_special_rows)
                {
                    int64_t row = dist(rng);
                    good_rows_set.insert(row);
                }
                std::unordered_set<int64_t> bad_rows_set;
                while(bad_rows_set.size() < num_special_rows)
                {
                    int64_t bad_row;
                    do
                    {
                        bad_row = dist(rng);
                    } while(good_rows_set.find(bad_row) != good_rows_set.end());
                    bad_rows_set.insert(bad_row);
                }
                good_rows_.clear();
                bad_rows_.clear();
                good_rows_.insert(good_rows_.end(), good_rows_set.begin(), good_rows_set.end());
                bad_rows_.insert(bad_rows_.end(), bad_rows_set.begin(), bad_rows_set.end());
                std::sort(good_rows_.begin(), good_rows_.end());
                std::sort(bad_rows_.begin(), bad_rows_.end());
                return Status::OK();
            }

            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) override
            {
                thread_local_data_.resize(num_threads);
                output_callback_ = std::move(output_callback);
                finished_callback_ = std::move(finished_callback);
                schedule_callback_ = std::move(schedule_callback);
                for(size_t i = 0; i < num_threads; i++)
                    RETURN_NOT_OK(schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); }));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:
#define FOR_EACH_COLUMN(F)                      \
            F(S_SUPPKEY)                        \
            F(S_NAME)                           \
            F(S_ADDRESS)                        \
            F(S_NATIONKEY)                      \
            F(S_PHONE)                          \
            F(S_ACCTBAL)                        \
            F(S_COMMENT)

#define MAKE_ENUM(col) col,
            struct SUPPLIER
            {
                enum
                {
                    FOR_EACH_COLUMN(MAKE_ENUM)
                    kNumCols,
                };
            };
#undef MAKE_ENUM
#define MAKE_STRING_MAP(col)                    \
            { #col, SUPPLIER::col },
            const std::unordered_map<std::string, int> name_map_ =
            {
                FOR_EACH_COLUMN(MAKE_STRING_MAP)
            };
#undef MAKE_STRING_MAP
#define MAKE_FN_ARRAY(col)                                              \
            [this](size_t thread_index) { return this->col(thread_index); },
            std::vector<GenerateColumnFn> generators_ =
            {
                FOR_EACH_COLUMN(MAKE_FN_ARRAY)
            };
#undef MAKE_FN_ARRAY
#undef FOR_EACH_COLUMN

            std::vector<std::shared_ptr<DataType>> types_ =
            {
                int32(),
                fixed_size_binary(25),
                utf8(),
                int32(),
                fixed_size_binary(15),
                decimal(12, 2),
                utf8(),
            };

            Status ProduceCallback(size_t thread_index)
            {
                if(done_.load())
                    return Status::OK();
                ThreadLocalData &tld = thread_local_data_[thread_index];
                tld.suppkey_start = rows_generated_.fetch_add(batch_size_);
                if(tld.suppkey_start >= rows_to_generate_)
                    return Status::OK();

                tld.to_generate = std::min(batch_size_,
                                           rows_to_generate_ - tld.suppkey_start);

                tld.batch.resize(SUPPLIER::kNumCols);
                std::fill(tld.batch.begin(), tld.batch.end(), Datum());
                for(int col : gen_list_)
                    RETURN_NOT_OK(generators_[col](thread_index));

                std::vector<Datum> result(gen_list_.size());
                for(size_t i = 0; i < gen_list_.size(); i++)
                {
                    int col_idx = gen_list_[i];
                    result[i] = tld.batch[col_idx];
                }
                ARROW_ASSIGN_OR_RAISE(ExecBatch eb, ExecBatch::Make(std::move(result)));
                int64_t batches_to_generate = (rows_to_generate_ + batch_size_ - 1) / batch_size_;
                int64_t batches_outputted_before_this_one = batches_outputted_.fetch_add(1);
                bool is_last_batch = batches_outputted_before_this_one == (batches_to_generate - 1);
                output_callback_(std::move(eb));
                if(is_last_batch)
                {
                    done_.store(true);
                    finished_callback_(batches_outputted_.load());
                    return Status::OK();
                }
                return schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); });
            }

            Status AllocateColumn(size_t thread_index, int column)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                ARROW_DCHECK(tld.batch[column].kind() == Datum::NONE);
                int32_t byte_width = arrow::internal::GetByteWidth(*types_[column]);
                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> buff, AllocateBuffer(tld.to_generate * byte_width));
                ArrayData ad(types_[column], tld.to_generate, { nullptr, std::move(buff) });
                tld.batch[column] = std::move(ad);
                return Status::OK();
            }

            Status S_SUPPKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_SUPPKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, SUPPLIER::S_SUPPKEY));
                    int32_t *s_suppkey = reinterpret_cast<int32_t *>(
                        tld.batch[SUPPLIER::S_SUPPKEY].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        s_suppkey[irow] = (tld.suppkey_start + irow + 1);
                    }
                }
                return Status::OK();
            }

            Status S_NAME(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_NAME].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(S_SUPPKEY(thread_index));
                    const int32_t *s_suppkey = reinterpret_cast<const int32_t *>(
                        tld.batch[SUPPLIER::S_SUPPKEY].array()->buffers[1]->data());
                    RETURN_NOT_OK(AllocateColumn(thread_index, SUPPLIER::S_NAME));
                    int32_t byte_width = arrow::internal::GetByteWidth(*types_[SUPPLIER::S_NAME]);
                    char *s_name = reinterpret_cast<char *>(
                        tld.batch[SUPPLIER::S_NAME].array()->buffers[1]->mutable_data());
                    // Look man, I'm just following the spec ok? Section 4.2.3 as of March 1 2022
                    const char *supplier = "Supplie#r";
                    const size_t supplier_length = std::strlen(supplier);
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        char *out = s_name + byte_width * irow;
                        std::strncpy(out, supplier, byte_width);
                        AppendNumberPaddedToNineDigits(out + supplier_length, s_suppkey[irow]);
                    }
                }
                return Status::OK();
            }

            Status S_ADDRESS(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_ADDRESS].kind() == Datum::NONE)
                {
                    ARROW_ASSIGN_OR_RAISE(
                        tld.batch[SUPPLIER::S_ADDRESS],
                        RandomVString(tld.rng, tld.to_generate, 10, 40));
                }
                return Status::OK();
            }

            Status S_NATIONKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_NATIONKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, SUPPLIER::S_NATIONKEY));
                    std::uniform_int_distribution<int32_t> dist(0, 24);
                    int32_t *s_nationkey = reinterpret_cast<int32_t *>(
                        tld.batch[SUPPLIER::S_NATIONKEY].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                        s_nationkey[irow] = dist(tld.rng);
                }
                return Status::OK();
            }

            Status S_PHONE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_PHONE].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(S_NATIONKEY(thread_index));
                    RETURN_NOT_OK(AllocateColumn(thread_index, SUPPLIER::S_PHONE));
                    int32_t byte_width = arrow::internal::GetByteWidth(*types_[SUPPLIER::S_PHONE]); 
                    const int32_t *s_nationkey = reinterpret_cast<const int32_t *>(
                        tld.batch[SUPPLIER::S_NATIONKEY].array()->buffers[1]->data());
                    char *s_phone = reinterpret_cast<char *>(
                        tld.batch[SUPPLIER::S_PHONE].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        GeneratePhoneNumber(
                            s_phone + irow * byte_width,
                            tld.rng,
                            s_nationkey[irow]);
                    }
                }
                return Status::OK();
            }

            Status S_ACCTBAL(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_ACCTBAL].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, SUPPLIER::S_ACCTBAL));
                    Decimal128 *s_acctbal = reinterpret_cast<Decimal128 *>(
                        tld.batch[SUPPLIER::S_ACCTBAL].array()->buffers[1]->mutable_data());
                    std::uniform_int_distribution<int64_t> dist(-99999, 999999);
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                        s_acctbal[irow] = { dist(tld.rng) };
                }
                return Status::OK();
            }

            Status S_COMMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[SUPPLIER::S_COMMENT].kind() == Datum::NONE)
                {
                    ARROW_ASSIGN_OR_RAISE(tld.batch[SUPPLIER::S_COMMENT], g_text.GenerateComments(tld.to_generate, 25, 100, tld.rng));
                    ModifyComments(thread_index, "Recommends", good_rows_);
                    ModifyComments(thread_index, "Complaints", bad_rows_);
                }
                return Status::OK();
            }

            void ModifyComments(
                size_t thread_index,
                const char *review,
                const std::vector<int64_t> &indices)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                const int32_t *offsets = reinterpret_cast<const int32_t *>(
                    tld.batch[SUPPLIER::S_COMMENT].array()->buffers[1]->data());
                char *str = reinterpret_cast<char *>(
                    tld.batch[SUPPLIER::S_COMMENT].array()->buffers[2]->mutable_data());
                const char *customer = "Customer";
                const size_t customer_length = std::strlen(customer);
                const size_t review_length = std::strlen(review);

                auto it = std::lower_bound(indices.begin(), indices.end(), tld.suppkey_start);
                for(; it != indices.end() && *it < tld.suppkey_start + tld.to_generate; it++)
                {
                    int64_t idx_in_batch = *it - tld.suppkey_start;
                    char *out = str + offsets[idx_in_batch];
                    int32_t str_length = offsets[idx_in_batch + 1] - offsets[idx_in_batch];
                    std::uniform_int_distribution<int32_t> gap_dist(0, str_length - customer_length - review_length);
                    int32_t gap = gap_dist(tld.rng);
                    int32_t total_length = customer_length + gap + review_length;
                    std::uniform_int_distribution<int32_t> start_dist(0, str_length - total_length);
                    int32_t start = start_dist(tld.rng);
                    std::memcpy(out + start, customer, customer_length);
                    std::memcpy(out + start + customer_length + gap, review, review_length);
                }
            }

            struct ThreadLocalData
            {
                random::pcg32_fast rng;
                int64_t suppkey_start;
                int64_t to_generate;
                std::vector<Datum> batch;
            };
            std::vector<ThreadLocalData> thread_local_data_;
            std::vector<int64_t> good_rows_;
            std::vector<int64_t> bad_rows_;

            OutputBatchCallback output_callback_;
            FinishedCallback finished_callback_;
            ScheduleCallback schedule_callback_;
            int64_t rows_to_generate_;
            std::atomic<int64_t> rows_generated_;
            float scale_factor_;
            int64_t batch_size_;
            std::vector<int> gen_list_;
            std::shared_ptr<Schema> schema_;
        };

        class PartGenerator : public TpchTableGenerator
        {
        public:
            PartGenerator(std::shared_ptr<PartAndPartSupplierGenerator> gen)
                : gen_(std::move(gen))
            {
            }

            Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) override
            {
                scale_factor_ = scale_factor;
                batch_size_ = batch_size;
                ARROW_ASSIGN_OR_RAISE(schema_,
                                      gen_->SetPartOutputColumns(columns));
                return Status::OK();
            }
            
            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) override
            {
                RETURN_NOT_OK(gen_->Init(num_threads, batch_size_, scale_factor_));
                output_callback_ = std::move(output_callback);
                finished_callback_ = std::move(finished_callback);
                schedule_callback_ = std::move(schedule_callback);

                for(size_t i = 0; i < num_threads; i++)
                    RETURN_NOT_OK(schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); }));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:
            Status ProduceCallback(size_t)
            {
                if(done_.load())
                    return Status::OK();
                ARROW_ASSIGN_OR_RAISE(util::optional<ExecBatch> maybe_batch,
                                      gen_->NextPartBatch());
                if(!maybe_batch.has_value())
                {
                    int64_t batches_generated = gen_->part_batches_generated();
                    if(batches_generated == batches_outputted_.load())
                    {
                        bool expected = false;
                        if(done_.compare_exchange_strong(expected, true))
                            finished_callback_(batches_outputted_.load());
                    }
                    return Status::OK();
                }
                ExecBatch batch = std::move(*maybe_batch);
                output_callback_(std::move(batch));
                batches_outputted_++;
                return schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); });
            }

            OutputBatchCallback output_callback_;
            FinishedCallback finished_callback_;
            ScheduleCallback schedule_callback_;
            int64_t batch_size_;
            float scale_factor_;
            std::shared_ptr<PartAndPartSupplierGenerator> gen_;
            std::shared_ptr<Schema> schema_;
        };

        class PartSuppGenerator : public TpchTableGenerator
        {
        public:
            PartSuppGenerator(std::shared_ptr<PartAndPartSupplierGenerator> gen)
                : gen_(std::move(gen))
            {
            }

            Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) override
            {
                scale_factor_ = scale_factor;
                batch_size_ = batch_size;
                ARROW_ASSIGN_OR_RAISE(schema_,
                                      gen_->SetPartSuppOutputColumns(columns));
                return Status::OK();
            }
            
            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) override
            {
                RETURN_NOT_OK(gen_->Init(num_threads, batch_size_, scale_factor_));
                output_callback_ = std::move(output_callback);
                finished_callback_ = std::move(finished_callback);
                schedule_callback_ = std::move(schedule_callback);

                for(size_t i = 0; i < num_threads; i++)
                    RETURN_NOT_OK(schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); }));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:
            Status ProduceCallback(size_t)
            {
                if(done_.load())
                    return Status::OK();
                ARROW_ASSIGN_OR_RAISE(util::optional<ExecBatch> maybe_batch,
                                      gen_->NextPartSuppBatch());
                if(!maybe_batch.has_value())
                {
                    int64_t batches_generated = gen_->partsupp_batches_generated();
                    if(batches_generated == batches_outputted_.load())
                    {
                        bool expected = false;
                        if(done_.compare_exchange_strong(expected, true))
                            finished_callback_(batches_outputted_.load());
                    }
                    return Status::OK();
                }
                ExecBatch batch = std::move(*maybe_batch);
                output_callback_(std::move(batch));
                batches_outputted_++;
                return schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); });
            }

            OutputBatchCallback output_callback_;
            FinishedCallback finished_callback_;
            ScheduleCallback schedule_callback_;
            int64_t batch_size_;
            float scale_factor_;
            std::shared_ptr<PartAndPartSupplierGenerator> gen_;
            std::shared_ptr<Schema> schema_;
        };

        class CustomerGenerator : public TpchTableGenerator
        {
        public:
            Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) override
            {
                scale_factor_ = scale_factor;
                batch_size_ = batch_size;
                rows_to_generate_ = scale_factor_ * 150000;
                rows_generated_.store(0);
                ARROW_ASSIGN_OR_RAISE(schema_, SetOutputColumns(
                                          columns,
                                          types_,
                                          name_map_,
                                          gen_list_));
                return Status::OK();
            }

            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) override
            {
                thread_local_data_.resize(num_threads);
                output_callback_ = std::move(output_callback);
                finished_callback_ = std::move(finished_callback);
                schedule_callback_ = std::move(schedule_callback);
                for(size_t i = 0; i < num_threads; i++)
                    RETURN_NOT_OK(schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); }));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:
#define FOR_EACH_COLUMN(F)                      \
            F(C_CUSTKEY)                        \
            F(C_NAME)                           \
            F(C_ADDRESS)                        \
            F(C_NATIONKEY)                      \
            F(C_PHONE)                          \
            F(C_ACCTBAL)                        \
            F(C_MKTSEGMENT)                     \
            F(C_COMMENT)

#define MAKE_ENUM(col) col,
            struct CUSTOMER
            {
                enum
                {
                    FOR_EACH_COLUMN(MAKE_ENUM)
                    kNumCols,
                };
            };
#undef MAKE_ENUM
#define MAKE_STRING_MAP(col)                    \
            { #col, CUSTOMER::col },
            const std::unordered_map<std::string, int> name_map_ =
            {
                FOR_EACH_COLUMN(MAKE_STRING_MAP)
            };
#undef MAKE_STRING_MAP
#define MAKE_FN_ARRAY(col)                                              \
            [this](size_t thread_index) { return this->col(thread_index); },
            std::vector<GenerateColumnFn> generators_ =
            {
                FOR_EACH_COLUMN(MAKE_FN_ARRAY)
            };
#undef MAKE_FN_ARRAY
#undef FOR_EACH_COLUMN

            std::vector<std::shared_ptr<DataType>> types_ =
            {
                int32(),
                utf8(),
                utf8(),
                int32(),
                fixed_size_binary(15),
                decimal(12, 2),
                fixed_size_binary(10),
                utf8(),
            };

            Status ProduceCallback(size_t thread_index)
            {
                if(done_.load())
                    return Status::OK();
                ThreadLocalData &tld = thread_local_data_[thread_index];
                tld.custkey_start = rows_generated_.fetch_add(batch_size_);
                if(tld.custkey_start >= rows_to_generate_)
                    return Status::OK();

                tld.to_generate = std::min(batch_size_,
                                           rows_to_generate_ - tld.custkey_start);

                tld.batch.resize(CUSTOMER::kNumCols);
                std::fill(tld.batch.begin(), tld.batch.end(), Datum());
                for(int col : gen_list_)
                    RETURN_NOT_OK(generators_[col](thread_index));

                std::vector<Datum> result(gen_list_.size());
                for(size_t i = 0; i < gen_list_.size(); i++)
                {
                    int col_idx = gen_list_[i];
                    result[i] = tld.batch[col_idx];
                }
                ARROW_ASSIGN_OR_RAISE(ExecBatch eb, ExecBatch::Make(std::move(result)));
                int64_t batches_to_generate = (rows_to_generate_ + batch_size_ - 1) / batch_size_;
                int64_t batches_generated_before_this_one = batches_outputted_.fetch_add(1);
                bool is_last_batch = batches_generated_before_this_one == (batches_to_generate - 1);
                output_callback_(std::move(eb));
                if(is_last_batch)
                {
                    bool expected = false;
                    if(done_.compare_exchange_strong(expected, true))
                    {
                        finished_callback_(batches_outputted_.load());
                    }
                    return Status::OK();
                }
                return schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); });
            }

            Status AllocateColumn(size_t thread_index, int column)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                ARROW_DCHECK(tld.batch[column].kind() == Datum::NONE);
                int32_t byte_width = arrow::internal::GetByteWidth(*types_[column]);
                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> buff, AllocateBuffer(tld.to_generate * byte_width));
                ArrayData ad(types_[column], tld.to_generate, { nullptr, std::move(buff) });
                tld.batch[column] = std::move(ad);
                return Status::OK();
            }

            Status C_CUSTKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_CUSTKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, CUSTOMER::C_CUSTKEY));
                    int32_t *c_custkey = reinterpret_cast<int32_t *>(
                        tld.batch[CUSTOMER::C_CUSTKEY].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        c_custkey[irow] = (tld.custkey_start + irow + 1);
                    }
                }
                return Status::OK();
            }

            Status C_NAME(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_NAME].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(C_CUSTKEY(thread_index));
                    const int32_t *c_custkey = reinterpret_cast<const int32_t *>(
                        tld.batch[CUSTOMER::C_CUSTKEY].array()->buffers[1]->data());
                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> offset_buff, AllocateBuffer((tld.to_generate + 1) * sizeof(int32_t)));
                    int32_t *offsets = reinterpret_cast<int32_t *>(offset_buff->mutable_data());
                    const char *customer = "Customer#";
                    const size_t customer_length = std::strlen(customer);
                    offsets[0] = 0;
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        int num_digits = GetNumDigits(c_custkey[irow]);
                        int num_chars = std::max(num_digits, 9);
                        offsets[irow + 1] = offsets[irow] + num_chars + customer_length;
                    }
                    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> str_buff, AllocateBuffer(offsets[tld.to_generate]));
                    char *str = reinterpret_cast<char *>(str_buff->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        char *out = str + offsets[irow];
                        std::memcpy(out, customer, customer_length);
                        AppendNumberPaddedToNineDigits(out + customer_length, c_custkey[irow]);
                    }
                    ArrayData ad(utf8(), tld.to_generate, { nullptr, std::move(offset_buff), std::move(str_buff) });
                    tld.batch[CUSTOMER::C_NAME] = std::move(ad);
                }
                return Status::OK();
            }

            Status C_ADDRESS(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_ADDRESS].kind() == Datum::NONE)
                {
                    ARROW_ASSIGN_OR_RAISE(
                        tld.batch[CUSTOMER::C_ADDRESS],
                        RandomVString(tld.rng, tld.to_generate, 10, 40));
                }
                return Status::OK();
            }

            Status C_NATIONKEY(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_NATIONKEY].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, CUSTOMER::C_NATIONKEY));
                    std::uniform_int_distribution<int32_t> dist(0, 24);
                    int32_t *c_nationkey = reinterpret_cast<int32_t *>(
                        tld.batch[CUSTOMER::C_NATIONKEY].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                        c_nationkey[irow] = dist(tld.rng);
                }
                return Status::OK();
            }

            Status C_PHONE(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_PHONE].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(C_NATIONKEY(thread_index));
                    RETURN_NOT_OK(AllocateColumn(thread_index, CUSTOMER::C_PHONE));
                    int32_t byte_width = arrow::internal::GetByteWidth(*types_[CUSTOMER::C_PHONE]); 
                    const int32_t *c_nationkey = reinterpret_cast<const int32_t *>(
                        tld.batch[CUSTOMER::C_NATIONKEY].array()->buffers[1]->data());
                    char *c_phone = reinterpret_cast<char *>(
                        tld.batch[CUSTOMER::C_PHONE].array()->buffers[1]->mutable_data());
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        GeneratePhoneNumber(
                            c_phone + irow * byte_width,
                            tld.rng,
                            c_nationkey[irow]);
                    }
                }
                return Status::OK();
            }

            Status C_ACCTBAL(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_ACCTBAL].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, CUSTOMER::C_ACCTBAL));
                    Decimal128 *c_acctbal = reinterpret_cast<Decimal128 *>(
                        tld.batch[CUSTOMER::C_ACCTBAL].array()->buffers[1]->mutable_data());
                    std::uniform_int_distribution<int64_t> dist(-99999, 999999);
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                        c_acctbal[irow] = { dist(tld.rng) };
                }
                return Status::OK();
            }

            Status C_MKTSEGMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_MKTSEGMENT].kind() == Datum::NONE)
                {
                    RETURN_NOT_OK(AllocateColumn(thread_index, CUSTOMER::C_MKTSEGMENT));
                    int32_t byte_width = arrow::internal::GetByteWidth(*types_[CUSTOMER::C_MKTSEGMENT]); 
                    char *c_mktsegment = reinterpret_cast<char *>(
                        tld.batch[CUSTOMER::C_MKTSEGMENT].array()->buffers[1]->mutable_data());
                    std::uniform_int_distribution<int32_t> dist(0, kNumSegments - 1);
                    for(int64_t irow = 0; irow < tld.to_generate; irow++)
                    {
                        char *out = c_mktsegment + irow * byte_width;
                        int str_idx = dist(tld.rng);
                        std::strncpy(out, Segments[str_idx], byte_width);
                    }
                }
                return Status::OK();
            }

            Status C_COMMENT(size_t thread_index)
            {
                ThreadLocalData &tld = thread_local_data_[thread_index];
                if(tld.batch[CUSTOMER::C_COMMENT].kind() == Datum::NONE)
                {
                    ARROW_ASSIGN_OR_RAISE(tld.batch[CUSTOMER::C_COMMENT], g_text.GenerateComments(tld.to_generate, 29, 116, tld.rng));
                }
                return Status::OK();
            }

            struct ThreadLocalData
            {
                random::pcg32_fast rng;
                int64_t custkey_start;
                int64_t to_generate;
                std::vector<Datum> batch;
            };
            std::vector<ThreadLocalData> thread_local_data_;

            OutputBatchCallback output_callback_;
            FinishedCallback finished_callback_;
            ScheduleCallback schedule_callback_;
            int64_t rows_to_generate_;
            std::atomic<int64_t> rows_generated_;
            float scale_factor_;
            int64_t batch_size_;
            std::vector<int> gen_list_;
            std::shared_ptr<Schema> schema_;
        };

        class OrdersGenerator : public TpchTableGenerator
        {
        public:
            OrdersGenerator(std::shared_ptr<OrdersAndLineItemGenerator> gen)
                : gen_(std::move(gen))
            {
            }

            Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) override
            {
                scale_factor_ = scale_factor;
                batch_size_ = batch_size;
                ARROW_ASSIGN_OR_RAISE(schema_,
                                      gen_->SetOrdersOutputColumns(columns));
                return Status::OK();
            }
            
            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) override
            {
                RETURN_NOT_OK(gen_->Init(num_threads, batch_size_, scale_factor_));
                output_callback_ = std::move(output_callback);
                finished_callback_ = std::move(finished_callback);
                schedule_callback_ = std::move(schedule_callback);

                for(size_t i = 0; i < num_threads; i++)
                    RETURN_NOT_OK(schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); }));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:
            Status ProduceCallback(size_t)
            {
                if(done_.load())
                    return Status::OK();
                ARROW_ASSIGN_OR_RAISE(util::optional<ExecBatch> maybe_batch,
                                      gen_->NextOrdersBatch());
                if(!maybe_batch.has_value())
                {
                    int64_t batches_generated = gen_->orders_batches_generated();
                    if(batches_generated == batches_outputted_.load())
                    {
                        bool expected = false;
                        if(done_.compare_exchange_strong(expected, true))
                            finished_callback_(batches_outputted_.load());
                    }
                    return Status::OK();
                }
                ExecBatch batch = std::move(*maybe_batch);
                output_callback_(std::move(batch));
                batches_outputted_++;
                return schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); });
            }

            OutputBatchCallback output_callback_;
            FinishedCallback finished_callback_;
            ScheduleCallback schedule_callback_;
            int64_t batch_size_;
            float scale_factor_;
            std::shared_ptr<OrdersAndLineItemGenerator> gen_;
            std::shared_ptr<Schema> schema_;
        };

        class LineitemGenerator : public TpchTableGenerator
        {
        public:
            LineitemGenerator(std::shared_ptr<OrdersAndLineItemGenerator> gen)
                : gen_(std::move(gen))
            {}

            Status Init(
                std::vector<std::string> columns,
                float scale_factor,
                int64_t batch_size) override
            {
                scale_factor_ = scale_factor;
                batch_size_ = batch_size;
                ARROW_ASSIGN_OR_RAISE(schema_,
                                      gen_->SetLineItemOutputColumns(columns));
                return Status::OK();
            }
            
            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback schedule_callback) override
            {
                RETURN_NOT_OK(gen_->Init(num_threads, batch_size_, scale_factor_));
                output_callback_ = std::move(output_callback);
                finished_callback_ = std::move(finished_callback);
                schedule_callback_ = std::move(schedule_callback);

                for(size_t i = 0; i < num_threads; i++)
                    RETURN_NOT_OK(schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); }));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:
            Status ProduceCallback(size_t)
            {
                if(done_.load())
                    return Status::OK();
                ARROW_ASSIGN_OR_RAISE(util::optional<ExecBatch> maybe_batch,
                                      gen_->NextLineItemBatch());
                if(!maybe_batch.has_value())
                {
                    int64_t batches_generated = gen_->lineitem_batches_generated();
                    if(batches_generated == batches_outputted_.load())
                    {
                        bool expected = false;
                        if(done_.compare_exchange_strong(expected, true))
                            finished_callback_(batches_outputted_.load());
                    }
                    return Status::OK();
                }
                ExecBatch batch = std::move(*maybe_batch);
                output_callback_(std::move(batch));
                batches_outputted_++;
                return schedule_callback_([this](size_t thread_index) { return this->ProduceCallback(thread_index); });
            }

            OutputBatchCallback output_callback_;
            FinishedCallback finished_callback_;
            ScheduleCallback schedule_callback_;
            int64_t batch_size_;
            float scale_factor_;
            std::shared_ptr<OrdersAndLineItemGenerator> gen_;
            std::shared_ptr<Schema> schema_;
        };

        class NationGenerator : public TpchTableGenerator
        {
        public:
            Status Init(
                std::vector<std::string> columns,
                float /*scale_factor*/,
                int64_t /*batch_size*/) override
            {
                ARROW_ASSIGN_OR_RAISE(schema_,
                                      SetOutputColumns(
                                          columns,
                                          types_,
                                          name_map_,
                                          column_indices_));
                return Status::OK();
            }

            Status StartProducing(
                size_t /*num_threads*/,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback /*schedule_task_callback*/) override
            {
                std::shared_ptr<Buffer> N_NATIONKEY_buffer = Buffer::Wrap(N_NATIONKEY, sizeof(N_NATIONKEY));
                ArrayData N_NATIONKEY_arraydata(int32(), kRowCount, { nullptr, std::move(N_NATIONKEY_buffer) });

                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> N_NAME_buffer, AllocateBuffer(kRowCount * kNameByteWidth));
                char *N_NAME = reinterpret_cast<char *>(N_NAME_buffer->mutable_data());
                for(size_t i = 0; i < kRowCount; i++)
                    std::strncpy(N_NAME + kNameByteWidth * i, country_names_[i], kNameByteWidth);
                ArrayData N_NAME_arraydata(fixed_size_binary(kNameByteWidth), kRowCount, { nullptr, std::move(N_NAME_buffer) });

                std::shared_ptr<Buffer> N_REGIONKEY_buffer = Buffer::Wrap(N_REGIONKEY, sizeof(N_REGIONKEY));
                ArrayData N_REGIONKEY_arraydata(int32(), kRowCount, { nullptr, std::move(N_REGIONKEY_buffer) });

                ARROW_ASSIGN_OR_RAISE(Datum N_COMMENT_datum, g_text.GenerateComments(kRowCount, 31, 114, rng_));

                std::vector<Datum> fields =
                    {
                        std::move(N_NATIONKEY_arraydata),
                        std::move(N_NAME_arraydata),
                        std::move(N_REGIONKEY_arraydata),
                        std::move(N_COMMENT_datum)
                    };

                std::vector<Datum> result;
                for(const int &col : column_indices_)
                    result.push_back(fields[col]);
                ARROW_ASSIGN_OR_RAISE(ExecBatch batch, ExecBatch::Make(std::move(result)));
                output_callback(std::move(batch));
                finished_callback(static_cast<int64_t>(1));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

        private:            
            random::pcg32_fast rng_;

            static constexpr size_t kRowCount = 25;
            static constexpr int32_t kNameByteWidth = 25;
            const int32_t N_NATIONKEY[kRowCount] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 };
            const char *country_names_[kRowCount] =
            {
                "ALGERIA", "ARGENTINA", "BRAZIL",
                "CANADA", "EGYPT", "ETHIOPIA",
                "FRANCE", "GERMANY", "INDIA",
                "INDONESIA", "IRAN", "IRAQ",
                "JAPAN", "JORDAN", "KENYA",
                "MOROCCO", "MOZAMBIQUE", "PERU",
                "CHINA", "ROMANIA", "SAUDI ARABIA",
                "VIETNAM", "RUSSIA", "UNITED KINGDOM",
                "UNITED STATES"
            };
            const int32_t N_REGIONKEY[kRowCount] = { 0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1 };

            struct NATION
            {
                enum
                {
                    N_NATIONKEY,
                    N_NAME,
                    N_REGIONKEY,
                    N_COMMENT,
                };
            };

            const std::unordered_map<std::string, int> name_map_ =
            {
                { "N_NATIONKEY", NATION::N_NATIONKEY },
                { "N_NAME", NATION::N_NAME },
                { "N_REGIONKEY", NATION::N_REGIONKEY },
                { "N_COMMENT", NATION::N_COMMENT },
            };

            std::vector<std::shared_ptr<DataType>> types_ =
            {
                int32(),
                fixed_size_binary(kNameByteWidth),
                int32(),
                utf8(),
            };

            std::shared_ptr<Schema> schema_;
            std::vector<int> column_indices_;
        };

        class RegionGenerator : public TpchTableGenerator
        {
        public:
            Status Init(
                std::vector<std::string> columns,
                float /*scale_factor*/,
                int64_t /*batch_size*/) override
            {
                ARROW_ASSIGN_OR_RAISE(schema_,
                                      SetOutputColumns(
                                          columns,
                                          types_,
                                          name_map_,
                                          column_indices_));
                return Status::OK();
            }

            Status StartProducing(
                size_t num_threads,
                OutputBatchCallback output_callback,
                FinishedCallback finished_callback,
                ScheduleCallback /*schedule_task_callback*/) override
            {
                std::shared_ptr<Buffer> R_REGIONKEY_buffer = Buffer::Wrap(R_REGIONKEY, sizeof(R_REGIONKEY));
                ArrayData R_REGIONKEY_arraydata(int32(), kRowCount, { nullptr, std::move(R_REGIONKEY_buffer) });

                ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> R_NAME_buffer, AllocateBuffer(kRowCount * kNameByteWidth));
                char *R_NAME_data = reinterpret_cast<char *>(R_NAME_buffer->mutable_data());
                for(size_t i = 0; i < kRowCount; i++)
                    std::strncpy(R_NAME_data + kNameByteWidth * i, region_names_[i], kNameByteWidth);
                ArrayData R_NAME_arraydata(types_[static_cast<int>(REGION::R_NAME)], kRowCount, { nullptr, std::move(R_NAME_buffer) });

                ARROW_ASSIGN_OR_RAISE(Datum R_COMMENT_datum, g_text.GenerateComments(kRowCount, 31, 115, rng_));

                std::vector<Datum> fields = { std::move(R_REGIONKEY_arraydata), std::move(R_NAME_arraydata), std::move(R_COMMENT_datum) };
                std::vector<Datum> result;
                for(const int &col : column_indices_)
                    result.push_back(fields[col]);
                ARROW_ASSIGN_OR_RAISE(ExecBatch batch, ExecBatch::Make(std::move(result)));
                output_callback(std::move(batch));
                finished_callback(static_cast<int64_t>(1));
                return Status::OK();
            }

            std::shared_ptr<Schema> schema() const override
            {
                return schema_;
            }

            random::pcg32_fast rng_;

            static constexpr size_t kRowCount = 5;
            static constexpr int32_t kNameByteWidth = 25;
            const int32_t R_REGIONKEY[kRowCount] = { 0, 1, 2, 3, 4 };
            const char *region_names_[kRowCount] =
            {
                "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"
            };

            struct REGION
            {
                enum
                {
                    R_REGIONKEY,
                    R_NAME,
                    R_COMMENT,
                    kNumColumns,
                };
            };

            const std::unordered_map<std::string, int> name_map_ =
            {
                { "R_REGIONKEY", REGION::R_REGIONKEY },
                { "R_NAME", REGION::R_NAME },
                { "R_COMMENT", REGION::R_COMMENT },
            };

            const std::vector<std::shared_ptr<DataType>> types_ =
            {
                int32(),
                fixed_size_binary(kNameByteWidth),
                utf8(),
            };

            std::shared_ptr<Schema> schema_;
            std::vector<int> column_indices_;
        };

        class TpchNode : public ExecNode
        {
        public:
            TpchNode(ExecPlan *plan,
                     std::unique_ptr<TpchTableGenerator> generator)
                : ExecNode(plan, {}, {}, generator->schema(), /*num_outputs=*/1),
                  generator_(std::move(generator))
            {
            }

            const char *kind_name() const override
            {
                return "TpchNode";
            }

            [[noreturn]]
            static void NoInputs()
            {
                Unreachable("TPC-H node should never have any inputs");
            }

            [[noreturn]]
            void InputReceived(ExecNode *, ExecBatch) override
            {
                NoInputs();
            }

            [[noreturn]]
            void ErrorReceived(ExecNode *, Status) override
            {
                NoInputs();
            }

            [[noreturn]]
            void InputFinished(ExecNode *, int) override
            {
                NoInputs();
            }

            Status StartProducing() override
            {
                finished_ = Future<>::Make();
                return generator_->StartProducing(
                    thread_indexer_.Capacity(),
                    [this](ExecBatch batch) { this->OutputBatchCallback(std::move(batch)); },
                    [this](int64_t num_batches) { this->FinishedCallback(num_batches); },
                    [this](std::function<Status(size_t)> func) -> Status { return this->ScheduleTaskCallback(std::move(func)); }
                    );
            }

            void PauseProducing(ExecNode *output) override {}
            void ResumeProducing(ExecNode *output) override {}

            void StopProducing(ExecNode *output) override
            {
                DCHECK_EQ(output, outputs_[0]);
                StopProducing();
            }

            void StopProducing() override
            {
                generator_->Abort([this]() { this->finished_.MarkFinished(); });
            }

            Future<> finished() override
            {
                return finished_;
            }

        private:
            void OutputBatchCallback(ExecBatch batch)
            {
                outputs_[0]->InputReceived(this, std::move(batch));
            }

            void FinishedCallback(int64_t total_num_batches)
            {
                outputs_[0]->InputFinished(this, static_cast<int>(total_num_batches));
                finished_.MarkFinished();
            }

            Status ScheduleTaskCallback(std::function<Status(size_t)> func)
            {
                auto executor = plan_->exec_context()->executor();
                if (executor)
                {
                    RETURN_NOT_OK(executor->Spawn([this, func]
                    {
                        size_t thread_index = thread_indexer_();
                        Status status = func(thread_index);
                        if (!status.ok())
                        {
                            StopProducing();
                            ErrorIfNotOk(status);
                            return;
                        }
                    }));
                }
                else
                {
                    return func(0);
                }
                return Status::OK();
            }

            std::unique_ptr<TpchTableGenerator> generator_;

            Future<> finished_ = Future<>::MakeFinished();
            ThreadIndexer thread_indexer_;
        };

        Result<TpchGen> TpchGen::Make(ExecPlan *plan, float scale_factor, int64_t batch_size)
        {
            TpchGen result(plan, scale_factor, batch_size);
            return result;
        }

        template <typename Generator>
        Result<ExecNode *> TpchGen::CreateNode(std::vector<std::string> columns)
        {
            std::unique_ptr<Generator> generator = arrow::internal::make_unique<Generator>();
            RETURN_NOT_OK(generator->Init(std::move(columns), scale_factor_, batch_size_));
            return plan_->EmplaceNode<TpchNode>(plan_, std::move(generator));
        }

        Result<ExecNode *> TpchGen::Supplier(std::vector<std::string> columns)
        {
            return CreateNode<SupplierGenerator>(std::move(columns));
        }

        Result<ExecNode *> TpchGen::Part(std::vector<std::string> columns)
        {
            if(!part_and_part_supp_generator_)
            {
                part_and_part_supp_generator_ = std::make_shared<PartAndPartSupplierGenerator>();
            }
            std::unique_ptr<PartGenerator> generator = arrow::internal::make_unique<PartGenerator>(part_and_part_supp_generator_);
            RETURN_NOT_OK(generator->Init(std::move(columns), scale_factor_, batch_size_));
            return plan_->EmplaceNode<TpchNode>(plan_, std::move(generator));
        }

        Result<ExecNode *> TpchGen::PartSupp(std::vector<std::string> columns)
        {
            if(!part_and_part_supp_generator_)
            {
                part_and_part_supp_generator_ = std::make_shared<PartAndPartSupplierGenerator>();
            }
            std::unique_ptr<PartSuppGenerator> generator = arrow::internal::make_unique<PartSuppGenerator>(part_and_part_supp_generator_);
            RETURN_NOT_OK(generator->Init(std::move(columns), scale_factor_, batch_size_));
            return plan_->EmplaceNode<TpchNode>(plan_, std::move(generator));
        }

        Result<ExecNode *> TpchGen::Customer(std::vector<std::string> columns)
        {
            return CreateNode<CustomerGenerator>(std::move(columns));
        }

        Result<ExecNode *> TpchGen::Orders(std::vector<std::string> columns)
        {
            if(!orders_and_line_item_generator_)
            {
                orders_and_line_item_generator_ = std::make_shared<OrdersAndLineItemGenerator>();
            }
            std::unique_ptr<OrdersGenerator> generator = arrow::internal::make_unique<OrdersGenerator>(orders_and_line_item_generator_);
            RETURN_NOT_OK(generator->Init(std::move(columns), scale_factor_, batch_size_));
            return plan_->EmplaceNode<TpchNode>(plan_, std::move(generator));
        }

        Result<ExecNode *> TpchGen::Lineitem(std::vector<std::string> columns)
        {
            if(!orders_and_line_item_generator_)
            {
                orders_and_line_item_generator_ = std::make_shared<OrdersAndLineItemGenerator>();
            }
            std::unique_ptr<LineitemGenerator> generator = arrow::internal::make_unique<LineitemGenerator>(orders_and_line_item_generator_);
            RETURN_NOT_OK(generator->Init(std::move(columns), scale_factor_, batch_size_));
            return plan_->EmplaceNode<TpchNode>(plan_, std::move(generator));
        }

        Result<ExecNode *> TpchGen::Nation(std::vector<std::string> columns)
        {
            return CreateNode<NationGenerator>(std::move(columns));
        }

        Result<ExecNode *> TpchGen::Region(std::vector<std::string> columns)
        {
            return CreateNode<RegionGenerator>(std::move(columns));
        }
    }
}
