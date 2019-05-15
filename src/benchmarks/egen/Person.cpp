/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

#include "EGenTables_stdafx.h"

using namespace TPCE;

// Percentages used in determining gender.
const int   iPercentGenderIsMale = 49;

/*
*   Initializes in-memory representation of names files.
*/
CPerson::CPerson(CInputFiles inputFiles
                ,TIdent iStartFromCustomer
                ,bool bCacheEnabled
                )
    : m_LastNames(inputFiles.LastNames)
    , m_MaleFirstNames(inputFiles.MaleFirstNames)
    , m_FemaleFirstNames(inputFiles.FemaleFirstNames)
    , m_bCacheEnabled(bCacheEnabled)
{
    if (m_bCacheEnabled)
    {
        m_iCacheSize = iDefaultLoadUnitSize;
        m_iCacheOffset = iTIdentShift + iStartFromCustomer;
        m_CacheLastName = new char* [m_iCacheSize];
        m_CacheFirstName = new char* [m_iCacheSize];
        for (int i=0; i<m_iCacheSize; i++)
        {
            m_CacheLastName[i] = NULL;
            m_CacheFirstName[i] = NULL;
        }
    }
}

/*
*   Deallocate in-memory representation of names files.
*/
CPerson::~CPerson()
{
    if (m_bCacheEnabled)
    {
        delete[] m_CacheLastName;
        delete[] m_CacheFirstName;
    }
}

/*
*   Resets the cache.
*/
void CPerson::InitNextLoadUnit(TIdent iCacheOffsetIncrement)
{
    if (m_bCacheEnabled)
    {
        m_iCacheOffset += iCacheOffsetIncrement;
        for (int i=0; i<m_iCacheSize; i++)
        {
            m_CacheLastName[i] = NULL;
            m_CacheFirstName[i] = NULL;
        }
    }
}

/*
*   Returns the last name for a particular customer id.
*   It'll always be the same for the same customer id.
*/
char* CPerson::GetLastName(TIdent CID)
{
    char *LastName = NULL;

    // We will sometimes get CID values that are outside the current
    // load unit (cached range).  We need to check for this case
    // and avoid the lookup (as we will segfault or get bogus data.)
    // NOTE: These out-of-LU values are expected, as CIDs chosen for
    // Account-Permission rows are selected at random from a wide
    // range of LUs and thus will not be in the cache.  Further, CIDs
    // (aka BIDs) for Broker rows are outside the current LU range
    // and thus not in the cache.
    TIdent index = CID - m_iCacheOffset;
    bool bCheckCache = (index >= 0 && index < m_iCacheSize);
    if (m_bCacheEnabled && bCheckCache)
    {
        LastName = m_CacheLastName[index];
    }

    if (LastName == NULL)
    {
        RNGSEED OldSeed;
        int     iThreshold;

        OldSeed = m_rnd.GetSeed();

        m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseLastName, (RNGSEED) CID ));

        //generate Threshold up to the value of the last key (first member in a pair)
        iThreshold = m_rnd.RndIntRange(0, m_LastNames->GetGreatestKey() - 1);
        LastName = (m_LastNames->GetRecord(iThreshold))->LAST_NAME;

        m_rnd.SetSeed( OldSeed );

        if (m_bCacheEnabled && bCheckCache)
        {
            m_CacheLastName[index] = LastName;
        }
    }

    return LastName;
}

/*
*   Returns the first name for a particular customer id.
*   Determines gender first.
*/
char* CPerson::GetFirstName(TIdent CID)
{
    char *FirstName = NULL;

    // cached before attempting to use the cache.
    // We will sometimes get CID values that are outside the current
    // load unit (cached range).  We need to check for this case
    // and avoid the lookup (as we will segfault or get bogus data.)
    // NOTE: These out-of-LU values are expected, as CIDs chosen for AP
    // rows are selected at random from a wide range of LUs and thus
    TIdent index = CID - m_iCacheOffset;
    bool bCheckCache = (index >= 0 && index < m_iCacheSize);
    if (m_bCacheEnabled && bCheckCache)
    {
        FirstName = m_CacheFirstName[index];
    }

    if (FirstName == NULL)
    {
        RNGSEED OldSeed;
        int     iThreshold;

        OldSeed = m_rnd.GetSeed();

        m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseFirstName, (RNGSEED) CID ));

        //Find out gender
        if (IsMaleGender(CID))
        {
            iThreshold = m_rnd.RndIntRange(0, m_MaleFirstNames->GetGreatestKey() - 1);
            FirstName = (m_MaleFirstNames->GetRecord(iThreshold))->FIRST_NAME;
        }
        else
        {
            iThreshold = m_rnd.RndIntRange(0, m_FemaleFirstNames->GetGreatestKey() - 1);
            FirstName = (m_FemaleFirstNames->GetRecord(iThreshold))->FIRST_NAME;
        }
        m_rnd.SetSeed( OldSeed );

        if (m_bCacheEnabled && bCheckCache)
        {
            m_CacheFirstName[index] = FirstName;
        }
    }
    return FirstName;
}
/*
*   Returns the middle name.
*/
char CPerson::GetMiddleName(TIdent CID)
{
    RNGSEED OldSeed;
    char    cMiddleInitial[2];

    OldSeed = m_rnd.GetSeed();
    m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseMiddleInitial, 
                   (RNGSEED) CID ));
    cMiddleInitial[1] = '\0';
    m_rnd.RndAlphaNumFormatted( cMiddleInitial, "a" );
    m_rnd.SetSeed( OldSeed );
    return( cMiddleInitial[0] );
}

/*
*   Returns the gender character for a particular customer id.
*/
char CPerson::GetGender(TIdent CID)
{
    RNGSEED OldSeed;
    char    cGender;

    OldSeed = m_rnd.GetSeed();
    m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseGender, (RNGSEED) CID ));

    //Find out gender
    if (m_rnd.RndPercent( iPercentGenderIsMale ))
    {
        cGender = 'M';
    }
    else
    {
        cGender = 'F';
    }

    m_rnd.SetSeed( OldSeed );
    return( cGender );
}

/*
*   Returns TRUE is a customer id is male
*/
bool CPerson::IsMaleGender(TIdent CID)
{
    return GetGender(CID)=='M';
}

/*
*   Generate tax id
*/
void CPerson::GetTaxID(TIdent CID, char *buf)
{
    RNGSEED OldSeed;

    OldSeed = m_rnd.GetSeed();

    // NOTE: the call to RndAlphaNumFormatted "consumes" an RNG value
    // for EACH character in the format string. Therefore, to avoid getting
    // tax ID's that overlap N-1 out of N characters, multiply the offset into
    // the sequence by N to get a unique range of values.
    m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseTaxID, 
                   ( (RNGSEED)CID * TaxIDFmt_len )));
    m_rnd.RndAlphaNumFormatted(buf, TaxIDFmt);
    m_rnd.SetSeed( OldSeed );
}

/*
*   Get first name, last name, and tax id.
*/
void CPerson::GetFirstLastAndTaxID(TIdent C_ID, char *szFirstName, char *szLastName, char *szTaxID)
{
    //Fill in the last name
    strncpy(szLastName, GetLastName(C_ID), cL_NAME_len);
    //Fill in the first name
    strncpy(szFirstName, GetFirstName(C_ID), cF_NAME_len);
    //Fill in the tax id
    GetTaxID(C_ID, szTaxID);
}


