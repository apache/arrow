//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/hold_ptr.hpp>
#include <boost/locale/localization_backend.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <memory>
#include <vector>

#ifdef BOOST_LOCALE_WITH_ICU
#    include "boost/locale/icu/icu_backend.hpp"
#endif

#ifndef BOOST_LOCALE_NO_POSIX_BACKEND
#    include "boost/locale/posix/posix_backend.hpp"
#endif

#ifndef BOOST_LOCALE_NO_STD_BACKEND
#    include "boost/locale/std/std_backend.hpp"
#endif

#ifndef BOOST_LOCALE_NO_WINAPI_BACKEND
#    include "boost/locale/win32/win_backend.hpp"
#endif

namespace boost { namespace locale {
    localization_backend::~localization_backend() = default;

    class localization_backend_manager::impl {
    public:
        impl(const impl& other) : default_backends_(other.default_backends_)
        {
            for(all_backends_type::const_iterator p = other.all_backends_.begin(); p != other.all_backends_.end(); ++p)
            {
                all_backends_type::value_type v;
                v.first = p->first;
                v.second.reset(p->second->clone());
                all_backends_.push_back(v);
            }
        }
        impl() : default_backends_(32, -1) {}

        impl& operator=(const impl&) = delete;

        localization_backend* create() const
        {
            std::vector<std::shared_ptr<localization_backend>> backends;
            for(unsigned i = 0; i < all_backends_.size(); i++)
                backends.push_back(all_backends_[i].second);
            return new actual_backend(backends, default_backends_);
        }
        void adopt_backend(const std::string& name, localization_backend* backend_ptr)
        {
            std::shared_ptr<localization_backend> sptr(backend_ptr);
            if(all_backends_.empty()) {
                all_backends_.push_back(std::make_pair(name, sptr));
                for(unsigned i = 0; i < default_backends_.size(); i++)
                    default_backends_[i] = 0;
            } else {
                for(unsigned i = 0; i < all_backends_.size(); i++)
                    if(all_backends_[i].first == name)
                        return;
                all_backends_.push_back(std::make_pair(name, sptr));
            }
        }

        void select(const std::string& backend_name, category_t category = all_categories)
        {
            unsigned id;
            for(id = 0; id < all_backends_.size(); ++id) {
                if(all_backends_[id].first == backend_name)
                    break;
            }
            if(id == all_backends_.size())
                return;
            category_t flag = category_first;
            for(unsigned i = 0; i < default_backends_.size(); ++flag, ++i) {
                if(category & flag) {
                    default_backends_[i] = id;
                }
            }
        }

        void remove_all_backends()
        {
            all_backends_.clear();
            for(unsigned i = 0; i < default_backends_.size(); i++) {
                default_backends_[i] = -1;
            }
        }
        std::vector<std::string> get_all_backends() const
        {
            std::vector<std::string> res;
            all_backends_type::const_iterator p;
            for(p = all_backends_.begin(); p != all_backends_.end(); ++p) {
                res.push_back(p->first);
            }
            return res;
        }

    private:
        class actual_backend : public localization_backend {
        public:
            actual_backend(const std::vector<std::shared_ptr<localization_backend>>& backends,
                           const std::vector<int>& index) :
                index_(index)
            {
                backends_.resize(backends.size());
                for(unsigned i = 0; i < backends.size(); i++) {
                    backends_[i].reset(backends[i]->clone());
                }
            }
            actual_backend* clone() const override { return new actual_backend(backends_, index_); }
            void set_option(const std::string& name, const std::string& value) override
            {
                for(unsigned i = 0; i < backends_.size(); i++)
                    backends_[i]->set_option(name, value);
            }
            void clear_options() override
            {
                for(unsigned i = 0; i < backends_.size(); i++)
                    backends_[i]->clear_options();
            }
            std::locale install(const std::locale& l, category_t category, char_facet_t type) override
            {
                unsigned id = 0;
                for(category_t v = category_first; v != category; ++v, ++id) {
                    if(v == category_last)
                        return l;
                }
                if(id >= index_.size() || index_[id] == -1)
                    return l;
                return backends_[index_[id]]->install(l, category, type);
            }

        private:
            std::vector<std::shared_ptr<localization_backend>> backends_;
            std::vector<int> index_;
        };

        typedef std::vector<std::pair<std::string, std::shared_ptr<localization_backend>>> all_backends_type;
        all_backends_type all_backends_;
        std::vector<int> default_backends_;
    };

    localization_backend_manager::localization_backend_manager() : pimpl_(new impl()) {}

    localization_backend_manager::~localization_backend_manager() = default;

    localization_backend_manager::localization_backend_manager(const localization_backend_manager& other) :
        pimpl_(new impl(*other.pimpl_))
    {}

    localization_backend_manager& localization_backend_manager::operator=(const localization_backend_manager& other)
    {
        pimpl_.reset(new impl(*other.pimpl_));
        return *this;
    }

    std::unique_ptr<localization_backend> localization_backend_manager::get() const
    {
        return std::unique_ptr<localization_backend>(pimpl_->create());
    }
    void localization_backend_manager::add_backend(const std::string& name,
                                                   std::unique_ptr<localization_backend> backend)
    {
        pimpl_->adopt_backend(name, backend.release());
    }

    localization_backend* localization_backend_manager::create() const
    {
        return pimpl_->create();
    }
    void localization_backend_manager::adopt_backend(const std::string& name, localization_backend* backend)
    {
        pimpl_->adopt_backend(name, backend);
    }

    void localization_backend_manager::remove_all_backends()
    {
        pimpl_->remove_all_backends();
    }
    std::vector<std::string> localization_backend_manager::get_all_backends() const
    {
        return pimpl_->get_all_backends();
    }
    void localization_backend_manager::select(const std::string& backend_name, category_t category)
    {
        pimpl_->select(backend_name, category);
    }

    namespace {
        // prevent initialization order fiasco
        boost::mutex& localization_backend_manager_mutex()
        {
            static boost::mutex the_mutex;
            return the_mutex;
        }
        // prevent initialization order fiasco
        localization_backend_manager& localization_backend_manager_global()
        {
            static localization_backend_manager the_manager;
            return the_manager;
        }

        struct init {
            init()
            {
                localization_backend_manager mgr;
#ifdef BOOST_LOCALE_WITH_ICU
                mgr.adopt_backend("icu", impl_icu::create_localization_backend());
#endif

#ifndef BOOST_LOCALE_NO_POSIX_BACKEND
                mgr.adopt_backend("posix", impl_posix::create_localization_backend());
#endif

#ifndef BOOST_LOCALE_NO_WINAPI_BACKEND
                mgr.adopt_backend("winapi", impl_win::create_localization_backend());
#endif

#ifndef BOOST_LOCALE_NO_STD_BACKEND
                mgr.adopt_backend("std", impl_std::create_localization_backend());
#endif

                localization_backend_manager::global(mgr);
            }
        } do_init;
    } // namespace

    localization_backend_manager localization_backend_manager::global()
    {
        boost::unique_lock<boost::mutex> lock(localization_backend_manager_mutex());
        localization_backend_manager mgr = localization_backend_manager_global();
        return mgr;
    }
    localization_backend_manager localization_backend_manager::global(const localization_backend_manager& in)
    {
        boost::unique_lock<boost::mutex> lock(localization_backend_manager_mutex());
        localization_backend_manager mgr = localization_backend_manager_global();
        localization_backend_manager_global() = in;
        return mgr;
    }

}} // namespace boost::locale
