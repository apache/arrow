/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef avro_parsing_Symbol_hh__
#define avro_parsing_Symbol_hh__

#include <vector>
#include <map>
#include <set>
#include <stack>
#include <sstream>

#include <boost/any.hpp>
#include <boost/tuple/tuple.hpp>

#include "Node.hh"
#include "Decoder.hh"
#include "Exception.hh"

namespace avro {
namespace parsing {

class Symbol;

typedef std::vector<Symbol> Production;
typedef std::shared_ptr<Production> ProductionPtr;
typedef boost::tuple<std::stack<ssize_t>, bool, ProductionPtr, ProductionPtr> RepeaterInfo;
typedef boost::tuple<ProductionPtr, ProductionPtr> RootInfo;

class Symbol {
public:
    enum Kind {
        sTerminalLow,   // extra has nothing
        sNull,
        sBool,
        sInt,
        sLong,
        sFloat,
        sDouble,
        sString,
        sBytes,
        sArrayStart,
        sArrayEnd,
        sMapStart,
        sMapEnd,
        sFixed,
        sEnum,
        sUnion,
        sTerminalHigh,
        sSizeCheck,     // Extra has size
        sNameList,      // Extra has a vector<string>
        sRoot,          // Root for a schema, extra is Symbol
        sRepeater,      // Array or Map, extra is symbol
        sAlternative,   // One of many (union), extra is Union
        sPlaceholder,   // To be fixed up later.
        sIndirect,      // extra is shared_ptr<Production>
        sSymbolic,      // extra is weal_ptr<Production>
        sEnumAdjust,
        sUnionAdjust,
        sSkipStart,
        sResolve,

        sImplicitActionLow,
        sRecordStart,
        sRecordEnd,
        sField,         // extra is string
        sRecord,
        sSizeList,
        sWriterUnion,
        sDefaultStart,  // extra has default value in Avro binary encoding
        sDefaultEnd,
        sImplicitActionHigh,
        sError
    };

private:
    Kind kind_;
    boost::any extra_;


    explicit Symbol(Kind k) : kind_(k) { }
    template <typename T> Symbol(Kind k, T t) : kind_(k), extra_(t) { }
public:

    Kind kind() const {
        return kind_;
    }

    template <typename T> T extra() const {
        return boost::any_cast<T>(extra_);
    }

    template <typename T> T* extrap() {
        return boost::any_cast<T>(&extra_);
    }

    template <typename T> const T* extrap() const {
        return boost::any_cast<T>(&extra_);
    }

    template <typename T> void extra(const T& t) {
        extra_ = t;
    }

    bool isTerminal() const {
        return kind_ > sTerminalLow && kind_ < sTerminalHigh;
    }

    bool isImplicitAction() const {
        return kind_ > sImplicitActionLow && kind_ < sImplicitActionHigh;
    }

    static const char* stringValues[];
    static const char* toString(Kind k) {
        return stringValues[k];
    }

    static Symbol rootSymbol(ProductionPtr& s)
    {
        return Symbol(Symbol::sRoot, RootInfo(s, std::make_shared<Production>()));
    }

    static Symbol rootSymbol(const ProductionPtr& main,
                             const ProductionPtr& backup)
    {
        return Symbol(Symbol::sRoot, RootInfo(main, backup));
    }

    static Symbol nullSymbol() {
        return Symbol(sNull);
    }

    static Symbol boolSymbol() {
        return Symbol(sBool);
    }

    static Symbol intSymbol() {
        return Symbol(sInt);
    }

    static Symbol longSymbol() {
        return Symbol(sLong);
    }

    static Symbol floatSymbol() {
        return Symbol(sFloat);
    }

    static Symbol doubleSymbol() {
        return Symbol(sDouble);
    }

    static Symbol stringSymbol() {
        return Symbol(sString);
    }

    static Symbol bytesSymbol() {
        return Symbol(sBytes);
    }

    static Symbol sizeCheckSymbol(size_t s) {
        return Symbol(sSizeCheck, s);
    }

    static Symbol fixedSymbol() {
        return Symbol(sFixed);
    }

    static Symbol enumSymbol() {
        return Symbol(sEnum);
    }

    static Symbol arrayStartSymbol() {
        return Symbol(sArrayStart);
    }

    static Symbol arrayEndSymbol() {
        return Symbol(sArrayEnd);
    }

    static Symbol mapStartSymbol() {
        return Symbol(sMapStart);
    }

    static Symbol mapEndSymbol() {
        return Symbol(sMapEnd);
    }

    static Symbol repeater(const ProductionPtr& p,
                           bool isArray) {
        return repeater(p, p, isArray);
    }

    static Symbol repeater(const ProductionPtr& read,
                           const ProductionPtr& skip,
                           bool isArray) {
        std::stack<ssize_t> s;
        return Symbol(sRepeater, RepeaterInfo(s, isArray, read, skip));
    }

    static Symbol defaultStartAction(std::shared_ptr<std::vector<uint8_t> > bb)
    {
        return Symbol(sDefaultStart, bb);
    }
 
    static Symbol defaultEndAction()
    {
        return Symbol(sDefaultEnd);
    }

    static Symbol alternative(
        const std::vector<ProductionPtr>& branches)
    {
        return Symbol(Symbol::sAlternative, branches);
    }

    static Symbol unionSymbol() {
        return Symbol(sUnion);
    }

    static Symbol recordStartSymbol() {
        return Symbol(sRecordStart);
    }

    static Symbol recordEndSymbol() {
        return Symbol(sRecordEnd);
    }

    static Symbol fieldSymbol(const std::string& name) {
        return Symbol(sField, name);
    }

    static Symbol writerUnionAction() {
        return Symbol(sWriterUnion);
    }

    static Symbol nameListSymbol(
        const std::vector<std::string>& v) {
        return Symbol(sNameList, v);
    }

    template <typename T>
    static Symbol placeholder(const T& n) {
        return Symbol(sPlaceholder, n);
    }

    static Symbol indirect(const ProductionPtr& p) {
        return Symbol(sIndirect, p);
    }

    static Symbol symbolic(const std::weak_ptr<Production>& p) {
        return Symbol(sSymbolic, p);
    }

    static Symbol enumAdjustSymbol(const NodePtr& writer,
        const NodePtr& reader);

    static Symbol unionAdjustSymbol(size_t branch,
                                    const ProductionPtr& p) {
        return Symbol(sUnionAdjust, std::make_pair(branch, p));
    }

    static Symbol sizeListAction(std::vector<size_t> order) {
        return Symbol(sSizeList, order);
    }

    static Symbol recordAction() {
        return Symbol(sRecord);
    }

    static Symbol error(const NodePtr& writer, const NodePtr& reader);

    static Symbol resolveSymbol(Kind w, Kind r) {
        return Symbol(sResolve, std::make_pair(w, r));
    }

    static Symbol skipStart() {
        return Symbol(sSkipStart);
    }

};

/**
 * Recursively replaces all placeholders in the production with the
 * corresponding values.
 */
template<typename T>
void fixup(const ProductionPtr& p,
           const std::map<T, ProductionPtr> &m)
{
    std::set<ProductionPtr> seen;
    for (Production::iterator it = p->begin(); it != p->end(); ++it) {
        fixup(*it, m, seen);
    }
}
    

/**
 * Recursively replaces all placeholders in the symbol with the values with the
 * corresponding values.
 */
template<typename T>
void fixup_internal(const ProductionPtr& p,
                    const std::map<T, ProductionPtr> &m,
                    std::set<ProductionPtr>& seen)
{
    if (seen.find(p) == seen.end()) {
        seen.insert(p);
        for (Production::iterator it = p->begin(); it != p->end(); ++it) {
            fixup(*it, m, seen);
        }
    }
}

template<typename T>
void fixup(Symbol& s, const std::map<T, ProductionPtr> &m,
           std::set<ProductionPtr>& seen)
{
    switch (s.kind()) {
    case Symbol::sIndirect:
        fixup_internal(s.extra<ProductionPtr>(), m, seen);
        break;
    case Symbol::sAlternative:
        {
            const std::vector<ProductionPtr> *vv =
            s.extrap<std::vector<ProductionPtr> >();
            for (std::vector<ProductionPtr>::const_iterator it = vv->begin();
                it != vv->end(); ++it) {
                fixup_internal(*it, m, seen);
            }
        }
        break;
    case Symbol::sRepeater:
        {
            const RepeaterInfo& ri = *s.extrap<RepeaterInfo>();
            fixup_internal(boost::tuples::get<2>(ri), m, seen);
            fixup_internal(boost::tuples::get<3>(ri), m, seen);
        }
        break;
    case Symbol::sPlaceholder:
        {
            typename std::map<T, std::shared_ptr<Production> >::const_iterator it =
                m.find(s.extra<T>());
            if (it == m.end()) {
                throw Exception("Placeholder symbol cannot be resolved");
            }
            s = Symbol::symbolic(std::weak_ptr<Production>(it->second));
        }
        break;
    case Symbol::sUnionAdjust:
        fixup_internal(s.extrap<std::pair<size_t, ProductionPtr> >()->second,
                       m, seen);
        break;
    default:
        break;
    }
}

template<typename Handler>
class SimpleParser {
    Decoder* decoder_;
    Handler& handler_;
    std::stack<Symbol> parsingStack;

    static void throwMismatch(Symbol::Kind actual, Symbol::Kind expected)
    {
        std::ostringstream oss;
        oss << "Invalid operation. Schema requires: " <<
            Symbol::toString(expected) << ", got: " <<
            Symbol::toString(actual);
        throw Exception(oss.str());
    }

    static void assertMatch(Symbol::Kind actual, Symbol::Kind expected)
    {
        if (expected != actual) {
            throwMismatch(actual, expected);
        }

    }

    void append(const ProductionPtr& ss) {
        for (Production::const_iterator it = ss->begin();
            it != ss->end(); ++it) {
            parsingStack.push(*it);
        }
    }

    size_t popSize() {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sSizeCheck, s.kind());
        size_t result = s.extra<size_t>();
        parsingStack.pop();
        return result;
    }

    static void assertLessThan(size_t n, size_t s) {
        if (n >= s) {
            std::ostringstream oss;
            oss << "Size max value. Upper bound: " << s << " found " << n;
            throw Exception(oss.str());
        }
    }

public:
    Symbol::Kind advance(Symbol::Kind k) {
        for (; ;) {
            Symbol& s = parsingStack.top();
//            std::cout << "advance: " << Symbol::toString(s.kind())
//                      << " looking for " << Symbol::toString(k) << '\n';
            if (s.kind() == k) {
                parsingStack.pop();
                return k;
            } else if (s.isTerminal()) {
                throwMismatch(k, s.kind());
            } else {
                switch (s.kind()) {
                case Symbol::sRoot:
                    append(boost::tuples::get<0>(*s.extrap<RootInfo>()));
                    continue;
                case Symbol::sIndirect:
                    {
                        ProductionPtr pp =
                            s.extra<ProductionPtr>();
                        parsingStack.pop();
                        append(pp);
                    }
                    continue;
                case Symbol::sSymbolic:
                    {
                        ProductionPtr pp(
                            s.extra<std::weak_ptr<Production> >());
                        parsingStack.pop();
                        append(pp);
                    }
                    continue;
                case Symbol::sRepeater:
                    {
                        RepeaterInfo *p = s.extrap<RepeaterInfo>();
                        std::stack<ssize_t>& ns = boost::tuples::get<0>(*p);
                        if (ns.empty()) {
                            throw Exception(
                                "Empty item count stack in repeater advance");
                        }
                        if (ns.top() == 0) {
                            throw Exception(
                                "Zero item count in repeater advance");
                        }
                        --ns.top();
                        append(boost::tuples::get<2>(*p));
                    }
                    continue;
                case Symbol::sError:
                    throw Exception(s.extra<std::string>());
                case Symbol::sResolve:
                    {
                        const std::pair<Symbol::Kind, Symbol::Kind>* p =
                            s.extrap<std::pair<Symbol::Kind, Symbol::Kind> >();
                        assertMatch(p->second, k);
                        Symbol::Kind result = p->first;
                        parsingStack.pop();
                        return result;
                    }
                case Symbol::sSkipStart:
                    parsingStack.pop();
                    skip(*decoder_);
                    break;
                default:
                    if (s.isImplicitAction()) {
                        size_t n = handler_.handle(s);
                        if (s.kind() == Symbol::sWriterUnion) {
                            parsingStack.pop();
                            selectBranch(n); 
                        } else {
                            parsingStack.pop();
                        }
                    } else {
                        std::ostringstream oss;
                        oss << "Encountered " << Symbol::toString(s.kind())
                            << " while looking for " << Symbol::toString(k);
                        throw Exception(oss.str());
                    }
                }
            }
        }
    }

    void skip(Decoder& d) {
        const size_t sz = parsingStack.size();
        if (sz == 0) {
            throw Exception("Nothing to skip!");
        }
        while (parsingStack.size() >= sz) {
            Symbol& t = parsingStack.top();
            // std::cout << "skip: " << Symbol::toString(t.kind()) << '\n';
            switch (t.kind()) {
            case Symbol::sNull:
                d.decodeNull();
                break;
            case Symbol::sBool:
                d.decodeBool();
                break;
            case Symbol::sInt:
                d.decodeInt();
                break;
            case Symbol::sLong:
                d.decodeLong();
                break;
            case Symbol::sFloat:
                d.decodeFloat();
                break;
            case Symbol::sDouble:
                d.decodeDouble();
                break;
            case Symbol::sString:
                d.skipString();
                break;
            case Symbol::sBytes:
                d.skipBytes();
                break;
            case Symbol::sArrayStart:
                {
                    parsingStack.pop();
                    size_t n = d.skipArray();
                    processImplicitActions();
                    assertMatch(Symbol::sRepeater, parsingStack.top().kind());
                    if (n == 0) {
                        break;
                    }
                    Symbol& t = parsingStack.top();
                    RepeaterInfo *p = t.extrap<RepeaterInfo>();
                    boost::tuples::get<0>(*p).push(n);
                    continue;
                }
            case Symbol::sArrayEnd:
                break;
            case Symbol::sMapStart:
                {
                    parsingStack.pop();
                    size_t n = d.skipMap();
                    processImplicitActions();
                    assertMatch(Symbol::sRepeater, parsingStack.top().kind());
                    if (n == 0) {
                        break;
                    }
                    Symbol& t = parsingStack.top();
                    RepeaterInfo *p = t.extrap<RepeaterInfo>();
                    boost::tuples::get<0>(*p).push(n);
                    continue;
                }
            case Symbol::sMapEnd:
                break;
            case Symbol::sFixed:
                {
                    parsingStack.pop();
                    Symbol& t = parsingStack.top();
                    d.decodeFixed(t.extra<size_t>());
                }
                break;
            case Symbol::sEnum:
                parsingStack.pop();
                d.decodeEnum();
                break;
            case Symbol::sUnion:
                {
                    parsingStack.pop();
                    size_t n = d.decodeUnionIndex();
                    selectBranch(n);
                    continue;
                }
            case Symbol::sRepeater:
                {
                    RepeaterInfo *p = t.extrap<RepeaterInfo>();
                    std::stack<ssize_t>& ns = boost::tuples::get<0>(*p);
                    if (ns.empty()) {
                        throw Exception(
                            "Empty item count stack in repeater skip");
                    }
                    ssize_t& n = ns.top();
                    if (n == 0) {
                        n = boost::tuples::get<1>(*p) ? d.arrayNext()
                                                      : d.mapNext();
                    }
                    if (n != 0) {
                        --n;
                        append(boost::tuples::get<3>(*p));
                        continue;
                    } else {
                        ns.pop();
                    }
                }
                break;
            case Symbol::sIndirect:
                {
                    ProductionPtr pp =
                        t.extra<ProductionPtr>();
                    parsingStack.pop();
                    append(pp);
                }
                continue;
            case Symbol::sSymbolic:
                {
                    ProductionPtr pp(
                        t.extra<std::weak_ptr<Production> >());
                    parsingStack.pop();
                    append(pp);
                }
                continue;
            default:
                {
                    std::ostringstream oss;
                    oss << "Don't know how to skip "
                        << Symbol::toString(t.kind());
                    throw Exception(oss.str());
                }
            }
            parsingStack.pop();
        }
    }

    void assertSize(size_t n) {
        size_t s = popSize();
        if (s != n) {
            std::ostringstream oss;
            oss << "Incorrect size. Expected: " << s << " found " << n;
            throw Exception(oss.str());
        }
    }

    void assertLessThanSize(size_t n) {
        assertLessThan(n, popSize());
    }

    size_t enumAdjust(size_t n) {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sEnumAdjust, s.kind());
        const std::pair<std::vector<int>, std::vector<std::string> >* v =
            s.extrap<std::pair<std::vector<int>, std::vector<std::string> > >();
        assertLessThan(n, v->first.size());

        int result = v->first[n];
        if (result < 0) {
            std::ostringstream oss;
            oss << "Cannot resolve symbol: " << v->second[-result - 1]
                << std::endl;
            throw Exception(oss.str());
        }
        parsingStack.pop();
        return result;
    }

    size_t unionAdjust() {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sUnionAdjust, s.kind());
        std::pair<size_t, ProductionPtr> p =
        s.extra<std::pair<size_t, ProductionPtr> >();
        parsingStack.pop();
        append(p.second);
        return p.first;
    }

    std::string nameForIndex(size_t e) {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sNameList, s.kind());
        const std::vector<std::string> names =
            s.extra<std::vector<std::string> >();
        if (e >= names.size()) {
            throw Exception("Not that many names");
        }
        std::string result = names[e];
        parsingStack.pop();
        return result;
    }

    size_t indexForName(const std::string &name) {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sNameList, s.kind());
        const std::vector<std::string> names =
            s.extra<std::vector<std::string> >();
        std::vector<std::string>::const_iterator it =
            std::find(names.begin(), names.end(), name);
        if (it == names.end()) {
            throw Exception("No such enum symbol");
        }
        size_t result = it - names.begin();
        parsingStack.pop();
        return result;
    }

    void pushRepeatCount(size_t n) {
        processImplicitActions();
        Symbol& s = parsingStack.top();
        assertMatch(Symbol::sRepeater, s.kind());
        RepeaterInfo *p = s.extrap<RepeaterInfo>();
        std::stack<ssize_t> &nn = boost::tuples::get<0>(*p);
        nn.push(n);
    }

    void nextRepeatCount(size_t n) {
        processImplicitActions();
        Symbol& s = parsingStack.top();
        assertMatch(Symbol::sRepeater, s.kind());
        RepeaterInfo *p = s.extrap<RepeaterInfo>();
        std::stack<ssize_t> &nn = boost::tuples::get<0>(*p);
        if (nn.empty() || nn.top() != 0) {
          throw Exception("Wrong number of items");
        }
        nn.top() = n;
    }

    void popRepeater() {
        processImplicitActions();
        Symbol& s = parsingStack.top();
        assertMatch(Symbol::sRepeater, s.kind());
        RepeaterInfo *p = s.extrap<RepeaterInfo>();
        std::stack<ssize_t> &ns = boost::tuples::get<0>(*p);
        if (ns.empty()) {
            throw Exception("Incorrect number of items (empty)");
        }
        if (ns.top() > 0) {
            throw Exception("Incorrect number of items (non-zero)");
        }
        ns.pop();
        parsingStack.pop();
    }

    void selectBranch(size_t n) {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sAlternative, s.kind());
        std::vector<ProductionPtr> v =
        s.extra<std::vector<ProductionPtr> >();
        if (n >= v.size()) {
            throw Exception("Not that many branches");
        }
        parsingStack.pop();
        append(v[n]);
    }

    const std::vector<size_t>& sizeList() {
        const Symbol& s = parsingStack.top();
        assertMatch(Symbol::sSizeList, s.kind());
        return *s.extrap<std::vector<size_t> >();
    }

    Symbol::Kind top() const {
        return parsingStack.top().kind();
    }

    void pop() {
        parsingStack.pop();
    }

    void processImplicitActions() {
        for (; ;) {
            Symbol& s = parsingStack.top();
            if (s.isImplicitAction()) {
                handler_.handle(s);
                parsingStack.pop();
            } else if (s.kind() == Symbol::sSkipStart) {
                parsingStack.pop();
                skip(*decoder_);
            } else {
                break;
            }
        }
    }

    SimpleParser(const Symbol& s, Decoder* d, Handler& h) :
        decoder_(d), handler_(h) {
        parsingStack.push(s);
    }

    void reset() {
        while (parsingStack.size() > 1) {
            parsingStack.pop();
        }
    }

};

inline std::ostream& operator<<(std::ostream& os, const Symbol s);

inline std::ostream& operator<<(std::ostream& os, const Production p)
{
    os << '(';
    for (Production::const_iterator it = p.begin(); it != p.end(); ++it) {
        os << *it << ", ";
    }
    os << ')';
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const Symbol s)
{
        switch (s.kind()) {
        case Symbol::sRepeater:
            {
                const RepeaterInfo& ri = *s.extrap<RepeaterInfo>();
                os << '(' << Symbol::toString(s.kind())
                   << ' ' << *boost::tuples::get<2>(ri)
                   << ' ' << *boost::tuples::get<3>(ri)
                   << ')';
            }
            break;
        case Symbol::sIndirect:
            {
                os << '(' << Symbol::toString(s.kind()) << ' '
                << *s.extra<std::shared_ptr<Production> >() << ')';
            }
            break;
        case Symbol::sAlternative:
            {
                os << '(' << Symbol::toString(s.kind());
                for (std::vector<ProductionPtr>::const_iterator it =
                         s.extrap<std::vector<ProductionPtr> >()->begin();
                     it != s.extrap<std::vector<ProductionPtr> >()->end();
                     ++it) {
                    os << ' ' << **it;
                }
                os << ')';
            }
            break;
        case Symbol::sSymbolic:
            {
              os << '(' << Symbol::toString(s.kind())
                 << ' ' << s.extra<std::weak_ptr<Production> >().lock()
                 << ')';
          }
          break;
        default:
            os << Symbol::toString(s.kind());
            break;
        }
        return os;
    }
}   // namespace parsing
}   // namespace avro

#endif
