using System;
using System.Collections.Generic;
using System.Text;

namespace December2025QueueSystemEducation.SampleExample.MediatorDesignPattern
{
    public class UserAddRequest:IUserAddRequest
    {
        public User user { get;}
        public UserAddRequest(User user)
        {
            this.user = user;
        }
    }
}
